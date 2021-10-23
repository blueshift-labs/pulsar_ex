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
      :batch_enabled,
      :batch_size,
      :flush_interval,
      :connect_interval,
      :connect_attempts,
      :max_connect_attempts,
      :batch_queue,
      :pending_queue,
      :pending_sends,
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
      :broker,
      :connection,
      :connection_ref,
      :producer_id,
      :producer_name,
      :producer_access_mode,
      :last_sequence_id,
      :max_message_size,
      :properties,
      :batch_enabled,
      :batch_size,
      :flush_interval,
      :connect_interval,
      :connect_attempts,
      :max_connect_attempts,
      :batch_queue,
      :pending_queue,
      :pending_sends,
      :producer_opts
    ]
  end

  use GenServer

  require Logger

  alias PulsarEx.{Topic, Admin, ConnectionManager, Connection, ProducerMessage}

  @batch_enabled false
  @batch_size 100
  @flush_interval 100
  @connect_interval 1000
  @max_connect_attempts 5

  def produce(pid, payload, message_opts, true) do
    GenServer.call(pid, {:produce, payload, message_opts})
  end

  def produce(pid, payload, message_opts, false) do
    GenServer.cast(pid, {:produce, payload, message_opts})
  end

  def start_link({%Topic{} = topic, producer_opts}) do
    GenServer.start_link(__MODULE__, {topic, producer_opts})
  end

  @impl true
  def init({%Topic{} = topic, producer_opts}) do
    Process.flag(:trap_exit, true)

    topic_name = Topic.to_name(topic)
    Logger.debug("Starting producer with topic #{topic_name}")

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
      producer_opts: producer_opts,
      batch_enabled: Keyword.get(producer_opts, :batch_enabled, @batch_enabled),
      batch_size: max(Keyword.get(producer_opts, :batch_size, @batch_size), 1),
      flush_interval: max(Keyword.get(producer_opts, :flush_interval, @flush_interval), 100),
      connect_interval:
        max(Keyword.get(producer_opts, :connect_interval, @connect_interval), 1000),
      connect_attempts: 0,
      max_connect_attempts:
        min(Keyword.get(producer_opts, :max_connect_attempts, @max_connect_attempts), 10),
      batch_queue: :queue.new(),
      pending_queue: :queue.new(),
      pending_sends: %{},
      metadata: metadata
    }

    Process.send(self(), :connect, [])

    if state.batch_enabled do
      Process.send_after(self(), :flush, state.flush_interval)
    end

    {:ok, state}
  end

  @impl true
  def handle_info(
        :connect,
        %{
          state: :connecting,
          connect_attempts: connect_attempts,
          max_connect_attempts: max_connect_attempts
        } = state
      )
      when connect_attempts >= max_connect_attempts do
    {:stop, {:error, :connection_error}, state}
  end

  @impl true
  def handle_info(:connect, %{state: :connecting} = state) do
    if state.connection_ref != nil do
      Process.demonitor(state.connection_ref)
    end

    state = %{state | connection_ref: nil}

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

      ref = Process.monitor(connection)

      metadata =
        properties
        |> Enum.into(%{}, fn {k, v} -> {String.to_atom(k), v} end)
        |> Map.merge(state.metadata)

      state = %{
        state
        | state: :ready,
          broker: broker,
          producer_id: producer_id,
          producer_name: producer_name,
          producer_access_mode: producer_access_mode,
          last_sequence_id: last_sequence_id,
          max_message_size: max_message_size,
          properties: properties,
          connection: connection,
          connection_ref: ref,
          metadata: metadata,
          connect_attempts: 0
      }

      Logger.debug("Connected producer #{state.producer_id} with topic #{state.topic_name}")

      :telemetry.execute(
        [:pulsar_ex, :producer, :connect, :success],
        %{count: 1, attempts: state.connect_attempts + 1},
        state.metadata
      )

      {:noreply, flush_pending_queue(state)}
    else
      err ->
        Logger.error("Error connecting producer with topic #{state.topic_name}, #{inspect(err)}")

        :telemetry.execute(
          [:pulsar_ex, :producer, :connect, :error],
          %{count: 1},
          state.metadata
        )

        state = %{state | connect_attempts: state.connect_attempts + 1}
        Process.send_after(self(), state.connect_interval * state.connect_attempts)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _, _, _, _}, %{topic_name: topic_name} = state) do
    Logger.error("Connection down for producer #{state.producer_id} with topic #{topic_name}")

    {:stop, {:error, :connection_down}, state}
  end

  @impl true
  def handle_info(:flush, %{state: :connecting} = state) do
    Process.send_after(self(), :flush, state.flush_interval)
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush, state) do
    state = flush_batch_queue(state, true)
    Process.send_after(self(), :flush, state.flush_interval)
    {:noreply, state}
  end

  @impl true
  def handle_call({:produce, payload, _}, _from, %{max_message_size: max_message_size} = state)
      when byte_size(payload) > max_message_size do
    {:reply, {:error, :message_size_too_large}, state}
  end

  @impl true
  def handle_call({:produce, payload, message_opts}, from, state) when is_list(message_opts) do
    handle_call({:produce, payload, map_message_opts(message_opts)}, from, state)
  end

  @impl true
  def handle_call({:produce, payload, %{} = message_opts}, from, %{state: :connecting} = state) do
    pending_queue = :queue.in({payload, message_opts, from}, state.pending_queue)
    {:noreply, %{state | pending_queue: pending_queue}}
  end

  @impl true
  def handle_call({:produce, payload, %{} = message_opts}, from, %{batch_enabled: false} = state) do
    {message, state} = create_message(payload, message_opts, state)
    reply = Connection.send_message(state.connection, message)

    case reply do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: 1},
          state.metadata
        )

        pending_sends = Map.put(state.pending_sends, message.sequence_id, from)
        {:noreply, %{state | pending_sends: pending_sends}}

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :error],
          %{count: 1},
          state.metadata
        )

        {:reply, reply, state}
    end
  end

  @impl true
  def handle_call({:produce, payload, %{deliver_at_time: nil} = message_opts}, from, state) do
    batch_queue = :queue.in({payload, message_opts, from}, state.batch_queue)
    {:noreply, flush_batch_queue(%{state | batch_queue: batch_queue}, false)}
  end

  @impl true
  def handle_call({:produce, payload, message_opts}, from, state) do
    {message, state} = create_message(payload, message_opts, state)
    reply = Connection.send_message(state.connection, message)

    case reply do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: 1},
          state.metadata
        )

        pending_sends = Map.put(state.pending_sends, message.sequence_id, from)
        {:noreply, %{state | pending_sends: pending_sends}}

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: 1},
          state.metadata
        )

        {:reply, reply, state}
    end
  end

  @impl true
  def handle_cast({:produce, payload, _}, %{max_message_size: max_message_size} = state)
      when byte_size(payload) > max_message_size do
    :telemetry.execute(
      [:pulsar_ex, :producer, :async_send, :error],
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
  def handle_cast({:produce, payload, %{} = message_opts}, %{state: :connecting} = state) do
    pending_queue = :queue.in({payload, message_opts, nil}, state.pending_queue)
    {:noreply, %{state | pending_queue: pending_queue}}
  end

  @impl true
  def handle_cast({:produce, payload, %{} = message_opts}, %{batch_enabled: false} = state) do
    {message, state} = create_message(payload, message_opts, state)
    reply = Connection.send_message(state.connection, message)

    case reply do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: 1},
          state.metadata
        )

        pending_sends = Map.put(state.pending_sends, message.sequence_id, nil)
        {:noreply, %{state | pending_sends: pending_sends}}

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :error],
          %{count: 1},
          state.metadata
        )

        {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:produce, payload, %{deliver_at_time: nil} = message_opts}, state) do
    batch_queue = :queue.in({payload, message_opts, nil}, state.batch_queue)
    {:noreply, flush_batch_queue(%{state | batch_queue: batch_queue}, false)}
  end

  @impl true
  def handle_cast({:produce, payload, message_opts}, state) do
    {message, state} = create_message(payload, message_opts, state)
    reply = Connection.send_message(state.connection, message)

    case reply do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: 1},
          state.metadata
        )

        pending_sends = Map.put(state.pending_sends, message.sequence_id, nil)
        {:noreply, %{state | pending_sends: pending_sends}}

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :error],
          %{count: 1},
          state.metadata
        )

        {:noreply, state}
    end
  end

  @impl true
  def handle_cast(:close, %{topic_name: topic_name} = state) do
    Logger.warn(
      "Received close command from connection for producer #{state.producer_id} with topic #{
        topic_name
      }"
    )

    Process.send_after(self(), :connect, state.connect_interval)

    :telemetry.execute(
      [:pulsar_ex, :producer, :close],
      %{count: 1},
      state.metadata
    )

    {:noreply, %{state | state: :connecting}}
  end

  @impl true
  def handle_cast({:send_receipt, sequence_id, receipt}, state) do
    {from, pending_sends} = Map.pop(state.pending_sends, sequence_id)
    reply(from, receipt)
    {:noreply, %{state | pending_sends: pending_sends}}
  end

  @impl true
  def terminate(reason, %{topic_name: topic_name} = state) do
    batch_len = :queue.len(state.batch_queue)
    pending_len = :queue.len(state.pending_queue)

    pending_sent =
      Map.values(state.pending_sends)
      |> Enum.reduce(0, fn
        froms, acc when is_list(froms) -> acc + length(froms)
        _, acc -> acc + 1
      end)

    if batch_len + pending_len + pending_sent > 0 do
      Logger.error(
        "Sending #{inspect(reason)} to #{batch_len + pending_len + pending_sent} remaining message for producer #{
          state.producer_id
        } with topic #{state.topic_name}"
      )

      fast_fail(reason, state.batch_queue)
      fast_fail(reason, state.pending_queue)

      state.pending_sends
      |> Map.values()
      |> Task.async_stream(&reply(&1, reason))
      |> Enum.count()
    end

    state = %{state | batch_queue: :queue.new()}

    case reason do
      :shutdown ->
        Logger.debug(
          "Stopping producer #{state.producer_id} with topic #{topic_name}, #{inspect(reason)}"
        )

        state

      :normal ->
        Logger.debug(
          "Stopping producer #{state.producer_id} with topic #{topic_name}, #{inspect(reason)}"
        )

        state

      {:shutdown, _} ->
        Logger.debug(
          "Stopping producer #{state.producer_id} with topic #{topic_name}, #{inspect(reason)}"
        )

        state

      _ ->
        Logger.error(
          "Stopping producer #{state.producer_id} with topic #{topic_name}, #{inspect(reason)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :producer, :exit],
          %{count: 1},
          state.metadata
        )

        state
    end
  end

  defp flush_pending_queue(%{batch_enabled: false} = state) do
    case :queue.out(state.pending_queue) do
      {:empty, {[], []}} ->
        state

      {{:value, {payload, message_opts, from}}, queue} ->
        {message, state} = create_message(payload, message_opts, state)
        reply = Connection.send_message(state.connection, message)

        case reply do
          :ok ->
            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :success],
              %{count: 1},
              state.metadata
            )

            pending_sends = Map.put(state.pending_sends, message.sequence_id, from)
            flush_pending_queue(%{state | pending_queue: queue, pending_sends: pending_sends})

          {:error, _} ->
            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :error],
              %{count: 1},
              state.metadata
            )

            reply(from, reply)
            flush_pending_queue(%{state | pending_queue: queue})
        end
    end
  end

  defp flush_pending_queue(state) do
    case :queue.out(state.pending_queue) do
      {:empty, {[], []}} ->
        state

      {{:value, {payload, %{deliver_at_time: nil} = message_opts, from}}, queue} ->
        batch_queue = :queue.in({payload, message_opts, from}, state.batch_queue)
        state = %{state | batch_queue: batch_queue}
        flush_pending_queue(%{state | pending_queue: queue, batch_queue: batch_queue})

      {{:value, {payload, message_opts, from}}, queue} ->
        {message, state} = create_message(payload, message_opts, state)
        reply = Connection.send_message(state.connection, message)

        case reply do
          :ok ->
            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :success],
              %{count: 1},
              state.metadata
            )

            pending_sends = Map.put(state.pending_sends, message.sequence_id, from)
            flush_pending_queue(%{state | pending_queue: queue, pending_sends: pending_sends})

          {:error, _} ->
            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :error],
              %{count: 1},
              state.metadata
            )

            reply(from, reply)
            flush_pending_queue(%{state | pending_queue: queue})
        end
    end
  end

  defp flush_batch_queue(state, force) do
    len = :queue.len(state.batch_queue)

    cond do
      len == 0 ->
        state

      :queue.len(state.batch_queue) < state.batch_size && !force ->
        state

      true ->
        {{[message | _] = messages, froms}, state} = create_messages(state.batch_queue, state)

        reply = Connection.send_messages(state.connection, messages)

        case reply do
          :ok ->
            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :success],
              %{count: length(messages)},
              state.metadata
            )

            pending_sends = Map.put(state.pending_sends, message.sequence_id, froms)
            %{state | pending_sends: pending_sends, batch_queue: :queue.new()}

          {:error, _} ->
            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :error],
              %{count: length(messages)},
              state.metadata
            )

            Task.async_stream(froms, &reply(&1, reply)) |> Enum.count()
            %{state | batch_queue: :queue.new()}
        end
    end
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

  defp create_messages(queue, state, messages \\ []) do
    case :queue.out(queue) do
      {:empty, {[], []}} ->
        {Enum.reverse(messages) |> Enum.unzip(), state}

      {{:value, {payload, message_opts, from}}, queue} ->
        {message, state} = create_message(payload, message_opts, state)
        create_messages(queue, state, [{message, from} | messages])
    end
  end

  defp fast_fail(reason, queue) do
    case :queue.out(queue) do
      {:empty, {[], []}} ->
        nil

      {{:value, {_, _, from}}, queue} ->
        reply(from, reason)
        fast_fail(reason, queue)
    end
  end

  defp reply(nil, _), do: nil

  defp reply(froms, reply) when is_list(froms),
    do: Task.async_stream(froms, &reply(&1, reply)) |> Enum.count()

  defp reply(from, reply), do: GenServer.reply(from, reply)
end
