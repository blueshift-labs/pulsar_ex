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
      :compaction_enabled,
      :flush_interval,
      :send_timeout,
      :queue,
      :queue_size,
      :max_queue_size,
      :batch,
      :pending_send,
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
      :compaction_enabled,
      :flush_interval,
      :send_timeout,
      :queue,
      :queue_size,
      :max_queue_size,
      :batch,
      :pending_send,
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

  @batch_enabled false
  @batch_size 100
  @compaction_enabled false
  @max_queue_size 1000
  @flush_interval 1000
  @connection_interval 3000
  @max_connection_attempts 10
  @send_timeout 5000

  def produce(pid, payload, message_opts) do
    GenServer.call(pid, {:produce, payload, message_opts}, :infinity)
  end

  def async_produce(pid, payload, message_opts) do
    GenServer.cast(pid, {:produce, payload, message_opts})
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
      compaction_enabled: Keyword.get(producer_opts, :compaction_enabled, @compaction_enabled),
      flush_interval: max(Keyword.get(producer_opts, :flush_interval, @flush_interval), 100),
      send_timeout: Keyword.get(producer_opts, :send_timeout, @send_timeout),
      queue: :queue.new(),
      queue_size: 0,
      batch: [],
      pending_send: nil,
      max_queue_size: min(Keyword.get(producer_opts, :max_queue_size, @max_queue_size), 10000),
      last_sequence_id: -1,
      producer_opts: producer_opts,
      connection_attempt: 0,
      max_connection_attempts:
        min(Keyword.get(producer_opts, :max_connection_attempts, @max_connection_attempts), 10)
    }

    Process.send(self(), :connect, [])
    Process.send_after(self(), :flush, state.flush_interval)

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

      {:noreply, state}
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
    state = flush(state, true)
    Process.send_after(self(), :flush, state.flush_interval)

    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:produce, _, _},
        _from,
        %{queue_size: queue_size, max_queue_size: max_queue_size} = state
      )
      when queue_size + 1 > max_queue_size do
    :telemetry.execute(
      [:pulsar_ex, :producer, :send, :error],
      %{count: 1},
      state.metadata
    )

    {:reply, {:error, :producer_queue_overflow}, state}
  end

  @impl true
  def handle_call({:produce, payload, message_opts}, from, state) when is_list(message_opts) do
    handle_call({:produce, payload, map_message_opts(message_opts)}, from, state)
  end

  @impl true
  def handle_call(
        {:produce, payload, %{deliver_at_time: nil} = message_opts},
        from,
        %{state: :connecting, batch_enabled: true} = state
      ) do
    batch = [{payload, message_opts, from} | state.batch]

    {queue, batch} =
      if length(batch) >= state.batch_size do
        queue = :queue.in(batch, state.queue)
        {queue, []}
      else
        {state.queue, batch}
      end

    {:noreply, %{state | queue_size: state.queue_size + 1, queue: queue, batch: batch}}
  end

  @impl true
  def handle_call({:produce, payload, message_opts}, from, %{state: :connecting} = state) do
    {queue, batch} =
      if state.batch == [] do
        queue = :queue.in({payload, message_opts, from}, state.queue)
        {queue, []}
      else
        queue = :queue.in(state.batch, state.queue)
        queue = :queue.in({payload, message_opts, from}, queue)
        {queue, []}
      end

    {:noreply, %{state | queue_size: state.queue_size + 1, queue: queue, batch: batch}}
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
    batch = [{payload, message_opts, from} | state.batch]

    {queue, batch} =
      if length(batch) >= state.batch_size do
        queue = :queue.in(batch, state.queue)
        {queue, []}
      else
        {state.queue, batch}
      end

    state = %{state | queue_size: state.queue_size + 1, queue: queue, batch: batch}
    {:noreply, flush(state, false)}
  end

  @impl true
  def handle_call({:produce, payload, message_opts}, from, state) do
    {queue, batch} =
      if state.batch == [] do
        queue = :queue.in({payload, message_opts, from}, state.queue)
        {queue, []}
      else
        queue = :queue.in(state.batch, state.queue)
        queue = :queue.in({payload, message_opts, from}, queue)
        {queue, []}
      end

    state = %{state | queue_size: state.queue_size + 1, queue: queue, batch: batch}
    {:noreply, flush(state, false)}
  end

  @impl true
  def handle_cast(
        {:produce, _, _},
        %{queue_size: queue_size, max_queue_size: max_queue_size} = state
      )
      when queue_size + 1 > max_queue_size do
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
  def handle_cast(
        {:produce, payload, %{deliver_at_time: nil} = message_opts},
        %{state: :connecting, batch_enabled: true} = state
      ) do
    batch = [{payload, message_opts, nil} | state.batch]

    {queue, batch} =
      if length(batch) >= state.batch_size do
        queue = :queue.in(batch, state.queue)
        {queue, []}
      else
        {state.queue, batch}
      end

    {:noreply, %{state | queue_size: state.queue_size + 1, queue: queue, batch: batch}}
  end

  @impl true
  def handle_cast({:produce, payload, message_opts}, %{state: :connecting} = state) do
    {queue, batch} =
      if state.batch == [] do
        queue = :queue.in({payload, message_opts, nil}, state.queue)
        {queue, []}
      else
        queue = :queue.in(state.batch, state.queue)
        queue = :queue.in({payload, message_opts, nil}, queue)
        {queue, []}
      end

    {:noreply, %{state | queue_size: state.queue_size + 1, queue: queue, batch: batch}}
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
  def handle_cast(
        {:produce, payload, %{deliver_at_time: nil} = message_opts},
        %{batch_enabled: true} = state
      ) do
    batch = [{payload, message_opts, nil} | state.batch]

    {queue, batch} =
      if length(batch) >= state.batch_size do
        queue = :queue.in(batch, state.queue)
        {queue, []}
      else
        {state.queue, batch}
      end

    state = %{state | queue_size: state.queue_size + 1, queue: queue, batch: batch}
    {:noreply, flush(state, false)}
  end

  @impl true
  def handle_cast({:produce, payload, message_opts}, state) do
    {queue, batch} =
      if state.batch == [] do
        queue = :queue.in({payload, message_opts, nil}, state.queue)
        {queue, []}
      else
        queue = :queue.in(state.batch, state.queue)
        queue = :queue.in({payload, message_opts, nil}, queue)
        {queue, []}
      end

    state = %{state | queue_size: state.queue_size + 1, queue: queue, batch: batch}
    {:noreply, flush(state, false)}
  end

  @impl true
  def handle_cast({:send_response, _}, %{pending_send: nil} = state) do
    Logger.warn(
      "Received stale send response in #{state.producer_id} for topic #{state.topic_name} from broker #{
        state.broker_name
      }"
    )

    :telemetry.execute(
      [:pulsar_ex, :producer, :send_response, :stale],
      %{count: 1},
      state.metadata
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:send_response, {sequence_id, _}}, %{pending_send: {seq_id, _, _}} = state)
      when sequence_id != seq_id do
    Logger.warn(
      "Received stale send response in #{state.producer_id} for topic #{state.topic_name} from broker #{
        state.broker_name
      }"
    )

    :telemetry.execute(
      [:pulsar_ex, :producer, :send_response, :stale],
      %{count: 1},
      state.metadata
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast(
        {:send_response, {_, reply}},
        %{pending_send: {_, {messages, froms}, {milli_ts, ts}}} = state
      )
      when is_list(messages) do
    duration = System.monotonic_time(:millisecond) - milli_ts

    Logger.debug(
      "Received send response in #{state.producer_id} for topic #{state.topic_name} from broker #{
        state.broker_name
      } after #{duration}ms"
    )

    case reply do
      {:ok, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send_response, :success],
          %{count: length(messages), duration: System.monotonic_time() - ts},
          state.metadata
        )

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send_response, :error],
          %{count: length(messages), duration: System.monotonic_time() - ts},
          state.metadata
        )
    end

    Enum.each(froms, &reply(&1, reply))

    {:noreply, %{state | pending_send: nil}}
  end

  @impl true
  def handle_cast(
        {:send_response, {_, reply}},
        %{pending_send: {_, {_, from}, {milli_ts, ts}}} = state
      ) do
    duration = System.monotonic_time(:millisecond) - milli_ts

    Logger.debug(
      "Received send response in #{state.producer_id} for topic #{state.topic_name} from broker #{
        state.broker_name
      } after #{duration}ms"
    )

    case reply do
      {:ok, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send_response, :success],
          %{count: 1, duration: System.monotonic_time() - ts},
          state.metadata
        )

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send_response, :error],
          %{count: 1, duration: System.monotonic_time() - ts},
          state.metadata
        )
    end

    reply(from, reply)

    {:noreply, %{state | pending_send: nil}}
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
    state = reply_all(state, reason)

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

  defp send_message({payload, message_opts, from}, state) do
    {message, state} = create_message(payload, message_opts, state)
    do_send_message(message, from, state)
  end

  defp do_send_message(%{sequence_id: sequence_id} = message, from, state) do
    milli_ts = System.monotonic_time(:millisecond)
    ts = System.monotonic_time()

    reply =
      Connection.send_message(
        state.connection,
        state.producer_id,
        sequence_id,
        message
      )

    case reply do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: 1, duration: System.monotonic_time() - ts},
          state.metadata
        )

        %{state | pending_send: {sequence_id, {message, from}, {milli_ts, ts}}}

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :error],
          %{count: 1},
          state.metadata
        )

        reply(from, reply)
        %{state | pending_send: nil}
    end
  end

  defp send_messages(batch, state) when is_list(batch) do
    {messages, _, state} =
      Enum.reduce(batch, {[], MapSet.new(), state}, fn {payload, message_opts, _},
                                                       {msgs, keys, acc} ->
        key = Map.get(message_opts, :ordering_key) || Map.get(message_opts, :partition_key)

        if state.compaction_enabled && key && MapSet.member?(keys, key) do
          {msgs, keys, acc}
        else
          {msg, acc} = create_message(payload, message_opts, acc)
          {[msg | msgs], MapSet.put(keys, key), acc}
        end
      end)

    froms = Enum.map(batch, fn {_, _, from} -> from end)
    do_send_messages(messages, froms, state)
  end

  defp do_send_messages([%{sequence_id: sequence_id} | _] = messages, froms, state) do
    milli_ts = System.monotonic_time(:millisecond)
    ts = System.monotonic_time()

    reply =
      Connection.send_messages(
        state.connection,
        state.producer_id,
        sequence_id,
        messages
      )

    case reply do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :success],
          %{count: length(messages), duration: System.monotonic_time() - ts},
          state.metadata
        )

        :telemetry.execute(
          [:pulsar_ex, :producer, :compacted],
          %{count: length(froms) - length(messages)},
          state.metadata
        )

        %{state | pending_send: {sequence_id, {messages, froms}, {milli_ts, ts}}}

      {:error, _} ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :error],
          %{count: length(messages)},
          state.metadata
        )

        Enum.each(froms, &reply(&1, reply))
        %{state | pending_send: nil}
    end
  end

  defp flush(%{pending_send: {_, {messages, froms}, {milli_ts, _}}} = state, _)
       when is_list(messages) do
    if System.monotonic_time(:millisecond) - milli_ts < state.send_timeout do
      state
    else
      :telemetry.execute(
        [:pulsar_ex, :producer, :send, :timeout],
        %{count: length(messages)},
        state.metadata
      )

      do_send_messages(messages, froms, state)
    end
  end

  defp flush(%{pending_send: {_, {message, from}, {milli_ts, _}}} = state, _) do
    if System.monotonic_time(:millisecond) - milli_ts < state.send_timeout do
      state
    else
      :telemetry.execute(
        [:pulsar_ex, :producer, :send, :timeout],
        %{count: 1},
        state.metadata
      )

      do_send_message(message, from, state)
    end
  end

  defp flush(state, force?) do
    case :queue.out(state.queue) do
      {:empty, _} ->
        if force? && state.batch != [] do
          state = send_messages(state.batch, state)
          %{state | queue_size: state.queue_size - length(state.batch), batch: []}
        else
          state
        end

      {{:value, {payload, message_opts, from}}, queue} ->
        state = send_message({payload, message_opts, from}, state)
        %{state | queue_size: state.queue_size - 1, queue: queue}

      {{:value, batch}, queue} ->
        state = send_messages(batch, state)
        %{state | queue_size: state.queue_size - length(batch), queue: queue}
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

  defp reply(nil, _reply), do: nil
  defp reply(from, reply), do: GenServer.reply(from, reply)

  defp reply_all(%{pending_send: {_, {_, froms}, _}} = state, reply) when is_list(froms) do
    Enum.each(froms, &reply(&1, reply))
    reply_all(%{state | pending_send: nil}, reply)
  end

  defp reply_all(%{pending_send: {_, {_, from}, _}} = state, reply) do
    reply(from, reply)
    reply_all(%{state | pending_send: nil}, reply)
  end

  defp reply_all(%{batch: []} = state, reply) do
    case :queue.out(state.queue) do
      {:empty, _} ->
        state

      {{:value, {_, _, from}}, queue} ->
        reply(from, reply)
        reply_all(%{state | queue: queue}, reply)

      {{:value, batch}, queue} ->
        batch
        |> Enum.reverse()
        |> Enum.each(fn {_, _, from} ->
          reply(from, reply)
        end)

        reply_all(%{state | queue: queue}, reply)
    end
  end

  defp reply_all(%{batch: batch} = state, reply) do
    batch
    |> Enum.reverse()
    |> Enum.each(fn {_, _, from} ->
      reply(from, reply)
    end)

    reply_all(%{state | batch: []}, reply)
  end
end
