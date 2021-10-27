defmodule PulsarEx.PartitionedProducer do
  defmodule State do
    @enforce_keys [
      :state,
      :brokers,
      :admin_port,
      :topic,
      :topic_name,
      :topic_logical_name,
      :partition,
      :metadata,
      :batch_enabled,
      :batch_size,
      :flush_interval,
      :batch_queue,
      :producer_opts
    ]
    defstruct [
      :state,
      :brokers,
      :admin_port,
      :topic,
      :topic_name,
      :topic_logical_name,
      :partition,
      :metadata,
      :batch_enabled,
      :batch_size,
      :flush_interval,
      :batch_queue,
      :producer_opts,
      :broker,
      :broker_name,
      :connection,
      :producer_id,
      :producer_name,
      :producer_access_mode,
      :last_sequence_id,
      :max_message_size,
      :properties
    ]
  end

  use GenServer

  require Logger

  alias PulsarEx.{Topic, Broker, Admin, ConnectionManager, Connection, ProducerMessage}

  @batch_enabled false
  @batch_size 100
  @flush_interval 100
  @send_timeout 180_000
  @termination_timeout 1000

  def produce(true, pid, payload, message_opts) do
    start = System.monotonic_time()

    reply = GenServer.call(pid, {:produce, payload, message_opts, start}, @send_timeout)

    :telemetry.execute(
      [:pulsar_ex, :producer, :debug],
      %{duration: System.monotonic_time() - start},
      %{}
    )

    reply
  end

  def produce(false, pid, payload, message_opts) do
    GenServer.cast(pid, {:produce, payload, message_opts})
  end

  def start_link({%Topic{} = topic, producer_opts}) do
    GenServer.start_link(__MODULE__, {topic, producer_opts})
  end

  @impl true
  def init({%Topic{} = topic, producer_opts}) do
    Process.flag(:trap_exit, true)

    topic_name = Topic.to_name(topic)
    Logger.debug("Starting producer for topic #{topic_name}")

    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    topic_logical_name = Topic.to_logical_name(topic)

    metadata =
      if topic.partition == nil do
        %{topic: topic_logical_name}
      else
        %{topic: topic_logical_name, partition: topic.partition}
      end

    state = %State{
      state: :connecting,
      brokers: brokers,
      admin_port: admin_port,
      topic: topic,
      topic_name: topic_name,
      topic_logical_name: Topic.to_logical_name(topic),
      partition: topic.partition,
      metadata: metadata,
      batch_enabled: Keyword.get(producer_opts, :batch_enabled, @batch_enabled),
      batch_size: max(Keyword.get(producer_opts, :batch_size, @batch_size), 1),
      flush_interval: max(Keyword.get(producer_opts, :flush_interval, @flush_interval), 100),
      batch_queue: [],
      producer_opts: producer_opts
    }

    Process.send(self(), :connect, [])
    {:ok, state}
  end

  @impl true
  def handle_info(:connect, %{state: :connecting} = state) do
    start = System.monotonic_time()

    with {:ok, broker} <- Admin.lookup_topic(state.brokers, state.admin_port, state.topic),
         {:ok, pool} <- ConnectionManager.get_connection(broker),
         {:ok, reply} <- create_producer(pool, Topic.to_name(state.topic), state.producer_opts) do
      %{
        producer_id: producer_id,
        producer_name: producer_name,
        producer_access_mode: producer_access_mode,
        last_sequence_id: last_sequence_id,
        max_message_size: max_message_size,
        properties: properties,
        connection: connection
      } = reply

      Process.monitor(connection)

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
          producer_id: producer_id,
          producer_name: producer_name,
          producer_access_mode: producer_access_mode,
          last_sequence_id: last_sequence_id,
          max_message_size: max_message_size,
          properties: properties,
          metadata: metadata
      }

      Logger.debug(
        "Connected producer #{state.producer_id} for topic #{state.topic_name} to broker #{
          state.broker_name
        }"
      )

      :telemetry.execute(
        [:pulsar_ex, :producer, :connect, :success],
        %{count: 1, duration: System.monotonic_time() - start},
        state.metadata
      )

      if state.batch_enabled do
        Process.send_after(self(), :flush, state.flush_interval)
      end

      {:noreply, state}
    else
      err ->
        Logger.error("Error connecting producer for topic #{state.topic_name}, #{inspect(err)}")

        :telemetry.execute(
          [:pulsar_ex, :producer, :connect, :error],
          %{count: 1},
          state.metadata
        )

        {:stop, err, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _, _, _, _}, %{topic_name: topic_name} = state) do
    Logger.error(
      "Connection down in producer #{state.producer_id} for topic #{topic_name} to broker #{
        state.broker_name
      }"
    )

    {:stop, {:error, :connection_down}, state}
  end

  @impl true
  def handle_info(:flush, state) do
    state = flush_batch_queue(state, true)
    Process.send_after(self(), :flush, state.flush_interval)

    {:noreply, state}
  end

  @impl true
  def handle_call({:produce, _, _, _}, _from, %{state: :connecting} = state) do
    :telemetry.execute(
      [:pulsar_ex, :producer, :send, :error],
      %{count: 1},
      state.metadata
    )

    {:reply, {:error, :producer_not_ready}, state}
  end

  @impl true
  def handle_call({:produce, payload, _, _}, _from, %{max_message_size: max_message_size} = state)
      when byte_size(payload) > max_message_size do
    :telemetry.execute(
      [:pulsar_ex, :producer, :send, :error],
      %{count: 1},
      state.metadata
    )

    {:reply, {:error, :message_size_too_large}, state}
  end

  @impl true
  def handle_call({:produce, payload, message_opts, req_ts}, from, state)
      when is_list(message_opts) do
    handle_call({:produce, payload, map_message_opts(message_opts), req_ts}, from, state)
  end

  @impl true
  def handle_call(
        {:produce, payload, %{} = message_opts, req_ts},
        _from,
        %{batch_enabled: false} = state
      ) do
    start = System.monotonic_time()

    :telemetry.execute(
      [:pulsar_ex, :producer, :debug],
      %{queue_time: System.monotonic_time() - req_ts},
      %{}
    )

    {message, state} = create_message(payload, message_opts, state)
    reply = Connection.send_message(state.connection, message)

    case reply do
      {:ok, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: 1, duration: System.monotonic_time() - start},
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
  def handle_call(
        {:produce, payload, %{deliver_at_time: nil} = message_opts, req_ts},
        from,
        state
      ) do
    :telemetry.execute(
      [:pulsar_ex, :producer, :debug],
      %{queue_time: System.monotonic_time() - req_ts},
      %{}
    )

    {message, state} = create_message(payload, message_opts, state)

    state =
      flush_batch_queue(%{state | batch_queue: [{message, from} | state.batch_queue]}, false)

    {:noreply, state}
  end

  @impl true
  def handle_call({:produce, payload, message_opts, req_ts}, _from, state) do
    start = System.monotonic_time()

    :telemetry.execute(
      [:pulsar_ex, :producer, :debug],
      %{queue_time: System.monotonic_time() - req_ts},
      %{}
    )

    {message, state} = create_message(payload, message_opts, state)
    reply = Connection.send_message(state.connection, message)

    case reply do
      {:ok, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: 1, duration: System.monotonic_time() - start},
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
  def handle_cast({:produce, _, _}, %{state: :connecting} = state) do
    :telemetry.execute(
      [:pulsar_ex, :producer, :send, :error],
      %{count: 1},
      state.metadata
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:produce, payload, _}, %{max_message_size: max_message_size} = state)
      when byte_size(payload) > max_message_size do
    :telemetry.execute(
      [:pulsar_ex, :producer, :send, :error],
      %{count: 1},
      state.metadata
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:produce, payload, message_opts}, state) when is_list(message_opts) do
    handle_cast({:produce, payload, map_message_opts(message_opts)}, state)
  end

  @impl true
  def handle_cast({:produce, payload, %{} = message_opts}, %{batch_enabled: false} = state) do
    start = System.monotonic_time()

    {message, state} = create_message(payload, message_opts, state)
    reply = Connection.send_message(state.connection, message)

    case reply do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: 1, duration: System.monotonic_time() - start},
          state.metadata
        )

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :error],
          %{count: 1},
          state.metadata
        )
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:produce, payload, %{deliver_at_time: nil} = message_opts}, state) do
    {message, state} = create_message(payload, message_opts, state)
    state = flush_batch_queue(%{state | batch_queue: [{message, nil} | state.batch_queue]}, false)

    {:noreply, state}
  end

  @impl true
  def handle_cast({:produce, payload, message_opts}, state) do
    start = System.monotonic_time()

    {message, state} = create_message(payload, message_opts, state)
    reply = Connection.send_message(state.connection, message)

    case reply do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: 1, duration: System.monotonic_time() - start},
          state.metadata
        )

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :error],
          %{count: 1},
          state.metadata
        )
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast(:close, state) do
    Logger.warn(
      "Received close command in producer #{state.producer_id} for topic #{state.topic_name} from broker #{
        state.broker_name
      }"
    )

    {:stop, {:shutdown, :closed}, state}
  end

  @impl true
  def terminate(reason, state) do
    {_, froms} = state.batch_queue |> Enum.reverse() |> Enum.unzip()

    case reason do
      :shutdown ->
        Logger.debug(
          "Stopping producer #{state.producer_id} for topic #{state.topic_name} from broker #{
            state.broker_name
          }, #{inspect(reason)}"
        )

        Enum.each(froms, &reply(&1, reason))

        state

      :normal ->
        Logger.debug(
          "Stopping producer #{state.producer_id} for topic #{state.topic_name} from broker #{
            state.broker_name
          }, #{inspect(reason)}"
        )

        Enum.each(froms, &reply(&1, reason))

        state

      {:shutdown, :closed} ->
        Logger.warn(
          "Stopping producer #{state.producer_id} for topic #{state.topic_name} from broker #{
            state.broker_name
          }, #{inspect(reason)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :producer, :closed],
          %{count: 1},
          state.metadata
        )

        Enum.each(froms, &reply(&1, {:error, :closed}))

        Process.sleep(@termination_timeout)
        state

      {:shutdown, _} ->
        Logger.debug(
          "Stopping producer #{state.producer_id} for topic #{state.topic_name} from broker #{
            state.broker_name
          }, #{inspect(reason)}"
        )

        Enum.each(froms, &reply(&1, reason))

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

        Enum.each(froms, &reply(&1, reason))

        state
    end
  end

  defp flush_batch_queue(%{batch_queue: []} = state, _), do: state

  defp flush_batch_queue(%{batch_queue: batch_queue, batch_size: batch_size} = state, false)
       when length(batch_queue) < batch_size,
       do: state

  defp flush_batch_queue(state, _) do
    start = System.monotonic_time()

    {messages, froms} = Enum.reverse(state.batch_queue) |> Enum.unzip()
    reply = Connection.send_messages(state.connection, messages)

    case reply do
      {:ok, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: length(messages), duration: System.monotonic_time() - start},
          state.metadata
        )

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :error],
          %{count: length(messages)},
          state.metadata
        )
    end

    Enum.each(froms, &reply(&1, reply))
    %{state | batch_queue: []}
  end

  defp create_producer(pool, topic_name, producer_opts) do
    connection = :poolboy.checkout(pool)
    :poolboy.checkin(pool, connection)

    Connection.create_producer(connection, topic_name, producer_opts)
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

  defp reply(nil, _), do: nil
  defp reply(from, reply), do: GenServer.reply(from, reply)
end
