defmodule PulsarEx.PartitionedProducer do
  defmodule State do
    @enforce_keys [
      :state,
      :brokers,
      :admin_port,
      :topic,
      :topic_name,
      :partition,
      :metadata,
      :batch_enabled,
      :batch_size,
      :flush_interval,
      :send_timeout,
      :queue,
      :queue_size,
      :last_sequence_id,
      :producer_opts,
      :connection_attempt,
      :max_connection_attempts
    ]
    defstruct [
      :state,
      :brokers,
      :admin_port,
      :topic,
      :topic_name,
      :partition,
      :metadata,
      :producer_id,
      :batch_enabled,
      :batch_size,
      :flush_interval,
      :send_timeout,
      :queue,
      :queue_size,
      :last_sequence_id,
      :producer_opts,
      :connection_attempt,
      :max_connection_attempts,
      :broker,
      :broker_name,
      :connection,
      :connection_ref,
      :producer_name,
      :producer_access_mode,
      :max_message_size,
      :properties
    ]
  end

  use GenServer

  require Logger

  alias PulsarEx.{Topic, Broker, Admin, ConnectionManager, Connection, ProducerMessage}

  @batch_enabled true
  @batch_size 100
  @flush_interval 1000
  @connection_interval 3000
  @max_connection_attempts 10
  @send_timeout :infinity

  def produce(pid, payload, message_opts) do
    GenServer.call(pid, {:produce, payload, message_opts}, :infinity)
  end

  def start_link({topic_name, partition, producer_opts}) do
    GenServer.start_link(__MODULE__, {topic_name, partition, producer_opts})
  end

  @impl true
  def init({topic_name, partition, producer_opts}) do
    Process.flag(:trap_exit, true)

    Logger.debug("Starting producer for topic #{topic_name}")

    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    {:ok, topic} = Topic.parse(topic_name)

    metadata =
      if partition == nil do
        %{topic: topic_name}
      else
        %{topic: topic_name, partition: partition}
      end

    state = %State{
      state: :connecting,
      brokers: brokers,
      admin_port: admin_port,
      topic: %{topic | partition: partition},
      topic_name: topic_name,
      partition: partition,
      metadata: metadata,
      batch_enabled: Keyword.get(producer_opts, :batch_enabled, @batch_enabled),
      batch_size: max(Keyword.get(producer_opts, :batch_size, @batch_size), 1),
      flush_interval: max(Keyword.get(producer_opts, :flush_interval, @flush_interval), 100),
      send_timeout: Keyword.get(producer_opts, :send_timeout, @send_timeout),
      queue: :queue.new(),
      queue_size: 0,
      last_sequence_id: -1,
      producer_opts: producer_opts,
      connection_attempt: 0,
      max_connection_attempts:
        min(Keyword.get(producer_opts, :max_connection_attempts, @max_connection_attempts), 10)
    }

    Process.send(self(), :connect, [])

    if state.batch_enabled do
      Process.send_after(self(), :flush, state.flush_interval)
    end

    {:ok, state}
  end

  @impl true
  def handle_info(:connect, %{state: :connecting} = state) do
    if state.connection_ref != nil do
      Process.demonitor(state.connection_ref)
    end

    state = %{state | connection_ref: nil}

    with {:ok, broker} <- Admin.lookup_topic(state.brokers, state.admin_port, state.topic),
         {:ok, connection} <- ConnectionManager.get_connection(broker),
         {:ok, reply} <-
           Connection.create_producer(
             connection,
             Topic.to_name(state.topic),
             state.producer_opts
           ) do
      %{
        producer_id: producer_id,
        producer_name: producer_name,
        max_message_size: max_message_size,
        producer_access_mode: producer_access_mode,
        properties: properties
      } = reply

      ref = Process.monitor(connection)

      metadata =
        properties
        |> Enum.into(%{}, fn {k, v} -> {String.to_atom(k), v} end)
        |> Map.merge(state.metadata)

      state = %{
        state
        | state: :connected,
          broker: broker,
          broker_name: Broker.to_name(broker),
          connection: connection,
          connection_ref: ref,
          producer_id: producer_id,
          producer_name: producer_name,
          producer_access_mode: producer_access_mode,
          max_message_size: max_message_size,
          properties: properties,
          metadata: metadata,
          connection_attempt: 0
      }

      Logger.debug(
        "Connected producer #{state.producer_id} for topic #{state.topic_name} to broker #{
          state.broker_name
        }"
      )

      :telemetry.execute(
        [:pulsar_ex, :producer, :connect, :success],
        %{count: 1},
        state.metadata
      )

      {:noreply, flush_queue(state, true)}
    else
      # This happens when connection first establish, simply retry
      {:error, :invalid_message} = err ->
        Logger.debug("Error connecting producer for topic #{state.topic_name}, #{inspect(err)}")

        Process.send_after(self(), :connect, @connection_interval)

        {:noreply, state}

      err ->
        Logger.error("Error connecting producer for topic #{state.topic_name}, #{inspect(err)}")

        :telemetry.execute(
          [:pulsar_ex, :producer, :connect, :error],
          %{count: 1},
          state.metadata
        )

        state = %{state | connection_attempt: state.connection_attempt + 1}

        if state.connection_attempt < state.max_connection_attempts do
          Process.send_after(self(), :connect, @connection_interval * state.connection_attempt)

          {:noreply, state}
        else
          {:stop, err, state}
        end
    end
  end

  @impl true
  def handle_info({:DOWN, _, _, _, _}, %{topic_name: topic_name} = state) do
    Logger.error(
      "Connection down in producer #{state.producer_id} for topic #{topic_name} to broker #{
        state.broker_name
      }"
    )

    :telemetry.execute(
      [:pulsar_ex, :producer, :connection_down],
      %{count: 1},
      state.metadata
    )

    Process.send_after(self(), :connect, @connection_interval)
    {:noreply, %{state | state: :connecting}}
  end

  @impl true
  def handle_info(:flush, %{state: :connecting} = state) do
    Process.send_after(self(), :flush, state.flush_interval)
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush, state) do
    state = flush_queue(state, true)
    Process.send_after(self(), :flush, state.flush_interval)

    {:noreply, state}
  end

  @impl true
  def handle_call({:produce, payload, message_opts}, from, state) when is_list(message_opts) do
    handle_call({:produce, payload, map_message_opts(message_opts)}, from, state)
  end

  @impl true
  def handle_call(
        {:produce, payload, message_opts},
        from,
        %{state: :connecting} = state
      ) do
    queue = :queue.in({payload, message_opts, from}, state.queue)
    {:noreply, %{state | queue_size: state.queue_size + 1, queue: queue}}
  end

  @impl true
  def handle_call({:produce, payload, _}, _from, %{max_message_size: max_message_size} = state)
      when byte_size(payload) > max_message_size do
    :telemetry.execute(
      [:pulsar_ex, :producer, :send, :error],
      %{count: 1},
      state.metadata
    )

    {:reply, {:error, :message_size_too_large}, state}
  end

  @impl true
  def handle_call(
        {:produce, payload, %{deliver_at_time: nil} = message_opts},
        from,
        %{batch_enabled: true} = state
      ) do
    queue = :queue.in({payload, message_opts, from}, state.queue)
    state = %{state | queue_size: state.queue_size + 1, queue: queue}
    {:noreply, flush_queue(state, false)}
  end

  @impl true
  def handle_call({:produce, payload, message_opts}, _from, state) do
    state = flush_queue(state, true)

    start = System.monotonic_time(:millisecond)

    {%{sequence_id: sequence_id} = message, state} = create_message(payload, message_opts, state)

    reply =
      Connection.send_message(
        state.connection,
        state.producer_id,
        sequence_id,
        message,
        state.send_timeout
      )

    case reply do
      {:ok, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: 1, duration: System.monotonic_time(:millisecond) - start},
          state.metadata
        )

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :error],
          %{count: 1},
          state.metadata
        )
    end

    {:reply, reply, state}
  end

  @impl true
  def handle_cast(:close, state) do
    Logger.warn(
      "Received close command in producer #{state.producer_id} for topic #{state.topic_name} from broker #{
        state.broker_name
      }"
    )

    :telemetry.execute(
      [:pulsar_ex, :producer, :close],
      %{count: 1},
      state.metadata
    )

    Process.send_after(self(), :connect, @connection_interval)
    {:noreply, %{state | state: :connecting}}
  end

  @impl true
  def terminate(reason, state) do
    reply_all(state.queue, reason)

    case reason do
      :shutdown ->
        Logger.debug(
          "Stopping producer #{state.producer_id} for topic #{state.topic_name} from broker #{
            state.broker_name
          }, #{inspect(reason)}"
        )

        state

      :normal ->
        Logger.debug(
          "Stopping producer #{state.producer_id} for topic #{state.topic_name} from broker #{
            state.broker_name
          }, #{inspect(reason)}"
        )

        state

      {:shutdown, _} ->
        Logger.debug(
          "Stopping producer #{state.producer_id} for topic #{state.topic_name} from broker #{
            state.broker_name
          }, #{inspect(reason)}"
        )

        state

      _ ->
        Logger.error(
          "Stopping producer #{state.producer_id} for topic #{state.topic_name} from broker #{
            state.broker_name
          }, #{inspect(reason)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :producer, :exit],
          %{count: 1},
          state.metadata
        )

        state
    end
  end

  defp dequeue(state, batch \\ []) do
    case {:queue.peek(state.queue), batch} do
      {:empty, _} ->
        {Enum.reverse(batch), state}

      {{:value, {_, %{deliver_at_time: nil}, _}}, _} ->
        {{:value, {payload, message_opts, from}}, queue} = :queue.out(state.queue)
        state = %{state | queue_size: state.queue_size - 1, queue: queue}
        {message, state} = create_message(payload, message_opts, state)
        dequeue(state, [{message, from} | batch])

      {{:value, _}, []} ->
        {{:value, {payload, message_opts, from}}, queue} = :queue.out(state.queue)
        state = %{state | queue_size: state.queue_size - 1, queue: queue}
        {message, state} = create_message(payload, message_opts, state)
        {[{message, from}], state}

      {{:value, _}, batch} ->
        {Enum.reverse(batch), state}
    end
  end

  defp flush_queue(%{queue_size: queue_size, batch_size: batch_size} = state, false)
       when queue_size < batch_size do
    state
  end

  defp flush_queue(state, force?) do
    {batch, state} = dequeue(state)

    case batch do
      [] ->
        state

      [{%{sequence_id: sequence_id} = message, from}] ->
        start = System.monotonic_time(:millisecond)

        reply =
          Connection.send_message(
            state.connection,
            state.producer_id,
            sequence_id,
            message,
            state.send_timeout
          )

        case reply do
          {:ok, _} ->
            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :success],
              %{count: 1, duration: System.monotonic_time(:millisecond) - start},
              state.metadata
            )

          {:error, _} ->
            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :error],
              %{count: 1},
              state.metadata
            )
        end

        GenServer.reply(from, reply)

        flush_queue(state, force?)

      _ ->
        {[%{sequence_id: sequence_id} | _] = messages, froms} = Enum.unzip(batch)

        start = System.monotonic_time(:millisecond)

        reply =
          Connection.send_messages(
            state.connection,
            state.producer_id,
            sequence_id,
            messages,
            state.send_timeout
          )

        case reply do
          {:ok, _} ->
            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :success],
              %{count: length(messages), duration: System.monotonic_time(:millisecond) - start},
              state.metadata
            )

          {:error, _} ->
            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :error],
              %{count: length(messages)},
              state.metadata
            )
        end

        Enum.each(froms, &GenServer.reply(&1, reply))

        flush_queue(state, force?)
    end
  end

  defp map_message_opts(%{} = message_opts), do: map_message_opts(Enum.into(message_opts, []))

  defp map_message_opts(message_opts) do
    deliver_at_time =
      case Keyword.get(message_opts, :delay) do
        nil ->
          Keyword.get(message_opts, :deliver_at_time)

        delay when is_integer(delay) ->
          Timex.add(Timex.now(), Timex.Duration.from_milliseconds(delay))

        %Timex.Duration{} = delay ->
          Timex.add(Timex.now(), delay)
      end

    %{
      properties: Keyword.get(message_opts, :properties),
      partition_key: Keyword.get(message_opts, :partition_key),
      ordering_key: Keyword.get(message_opts, :ordering_key),
      event_time: Keyword.get(message_opts, :event_time),
      deliver_at_time: deliver_at_time
    }
  end

  defp create_message(payload, message_opts, state) do
    message = %ProducerMessage{
      producer_id: state.producer_id,
      producer_name: state.producer_name,
      sequence_id: state.last_sequence_id + 1,
      payload: payload,
      properties: Map.get(message_opts, :properties),
      partition_key: Map.get(message_opts, :partition_key),
      ordering_key: Map.get(message_opts, :ordering_key),
      event_time: Map.get(message_opts, :event_time),
      deliver_at_time: Map.get(message_opts, :deliver_at_time)
    }

    state = %{state | last_sequence_id: state.last_sequence_id + 1}

    {message, state}
  end

  defp reply_all(queue, reply) do
    case :queue.out(queue) do
      {:empty, _} ->
        nil

      {{:value, {_, _, from}}, queue} ->
        GenServer.reply(from, reply)
        reply_all(queue, reply)
    end
  end
end
