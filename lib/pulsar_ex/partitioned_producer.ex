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
      :send_timeout,
      :termination_timeout,
      :queue,
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
      :producer_id,
      :producer_name,
      :producer_access_mode,
      :last_sequence_id,
      :max_message_size,
      :properties,
      :batch_enabled,
      :batch_size,
      :flush_interval,
      :send_timeout,
      :termination_timeout,
      :queue,
      :producer_opts
    ]
  end

  use GenServer

  require Logger

  alias PulsarEx.{Topic, Admin, ConnectionManager, Connection, ProducerMessage}

  @batch_enabled false
  @batch_size 100
  @flush_interval 100
  @send_timeout 5_000
  @termination_timeout 1000

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
      state: :init,
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
      send_timeout: min(Keyword.get(producer_opts, :send_timeout, @send_timeout), 10_000),
      termination_timeout:
        min(Keyword.get(producer_opts, :termination_timeout, @termination_timeout), 5_000),
      queue: :queue.new(),
      metadata: metadata
    }

    Process.send(self(), :init, [])
    {:ok, state}
  end

  defp create_producer(pool, topic_name, producer_opts) do
    connection = :poolboy.checkout(pool)
    :poolboy.checkin(pool, connection)

    Connection.create_producer(connection, topic_name, producer_opts)
  end

  @impl true
  def handle_info(:init, %{state: :init} = state) do
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
        | state: :ready,
          broker: broker,
          producer_id: producer_id,
          producer_name: producer_name,
          producer_access_mode: producer_access_mode,
          last_sequence_id: last_sequence_id,
          max_message_size: max_message_size,
          properties: properties,
          connection: connection,
          metadata: metadata
      }

      if state.batch_enabled do
        Process.send_after(self(), :flush, state.flush_interval)
      end

      Logger.debug("Initialized producer #{state.producer_id} with topic #{state.topic_name}")

      :telemetry.execute(
        [:pulsar_ex, :producer, :init, :success],
        %{count: 1},
        metadata
      )

      {:noreply, state}
    else
      err ->
        :telemetry.execute(
          [:pulsar_ex, :producer, :init, :error],
          %{count: 1},
          state.metadata
        )

        {:stop, err, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _, _, _, _}, %{topic_name: topic_name} = state) do
    Logger.error("Connection down for producer #{state.producer_id} with topic #{topic_name}")

    {:stop, {:error, :connection_down}, state}
  end

  @impl true
  def handle_info(:flush, state) do
    if :queue.len(state.queue) > 0 do
      start = System.monotonic_time()

      {messages, froms} = :queue.to_list(state.queue) |> Enum.unzip()

      reply = Connection.send_messages(state.connection, messages, state.send_timeout)
      Task.async_stream(froms, &reply(&1, reply)) |> Enum.count()

      :telemetry.execute(
        [:pulsar_ex, :producer, :flush],
        %{count: 1, messages: length(messages), duration: System.monotonic_time() - start},
        state.metadata
      )

      Process.send_after(self(), :flush, state.flush_interval)

      {:noreply, %{state | queue: :queue.new()}}
    else
      Process.send_after(self(), :flush, state.flush_interval)

      {:noreply, state}
    end
  end

  @impl true
  def handle_call({:produce, _, _}, _from, %{state: :init} = state) do
    {:reply, {:error, :producer_not_ready}, state}
  end

  @impl true
  def handle_call({:produce, payload, message_opts}, from, state) when is_list(message_opts) do
    handle_call({:produce, payload, map_message_opts(message_opts)}, from, state)
  end

  @impl true
  def handle_call({:produce, payload, %{} = message_opts}, _from, %{batch_enabled: false} = state) do
    start = System.monotonic_time()

    {message, state} = create_message(payload, message_opts, state)
    reply = Connection.send_message(state.connection, message, state.send_timeout)

    :telemetry.execute(
      [:pulsar_ex, :producer, :send],
      %{count: 1, duration: System.monotonic_time() - start},
      state.metadata
    )

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:produce, payload, %{deliver_at_time: nil} = message_opts}, from, state) do
    {message, state} = create_message(payload, message_opts, state)
    queue = :queue.in({message, from}, state.queue)

    cond do
      :queue.len(queue) < state.batch_size ->
        {:noreply, %{state | queue: queue}}

      true ->
        start = System.monotonic_time()

        {messages, froms} = :queue.to_list(queue) |> Enum.unzip()

        reply = Connection.send_messages(state.connection, messages, state.send_timeout)
        Task.async_stream(froms, &reply(&1, reply)) |> Enum.count()

        :telemetry.execute(
          [:pulsar_ex, :producer, :flush],
          %{count: 1, messages: length(messages), duration: System.monotonic_time() - start},
          state.metadata
        )

        {:noreply, %{state | queue: :queue.new()}}
    end
  end

  @impl true
  def handle_call({:produce, payload, message_opts}, _from, state) do
    start = System.monotonic_time()

    {message, state} = create_message(payload, message_opts, state)
    reply = Connection.send_message(state.connection, message, state.send_timeout)

    :telemetry.execute(
      [:pulsar_ex, :producer, :send],
      %{count: 1, duration: System.monotonic_time() - start},
      state.metadata
    )

    {:reply, reply, state}
  end

  @impl true
  def handle_cast(:close, %{topic_name: topic_name} = state) do
    Logger.warn(
      "Received close command from connection for producer #{state.producer_id} with topic #{
        topic_name
      }"
    )

    {:stop, {:shutdown, :close}, state}
  end

  @impl true
  def handle_cast({:produce, _, _}, %{state: :init} = state) do
    Logger.error("Producer not ready with topic #{state.topic_name}")
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
    Connection.send_message(state.connection, message, state.send_timeout)

    :telemetry.execute(
      [:pulsar_ex, :producer, :send],
      %{count: 1, duration: System.monotonic_time() - start},
      state.metadata
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:produce, payload, %{deliver_at_time: nil} = message_opts}, state) do
    {message, state} = create_message(payload, message_opts, state)
    queue = :queue.in({message, nil}, state.queue)

    cond do
      :queue.len(queue) < state.batch_size ->
        {:noreply, %{state | queue: queue}}

      true ->
        start = System.monotonic_time()

        {messages, froms} = :queue.to_list(queue) |> Enum.unzip()

        reply = Connection.send_messages(state.connection, messages, state.send_timeout)
        Task.async_stream(froms, &reply(&1, reply)) |> Enum.count()

        :telemetry.execute(
          [:pulsar_ex, :producer, :flush],
          %{count: 1, messages: length(messages), duration: System.monotonic_time() - start},
          state.metadata
        )

        {:noreply, %{state | queue: :queue.new()}}
    end
  end

  @impl true
  def handle_cast({:produce, payload, message_opts}, state) do
    start = System.monotonic_time()

    {message, state} = create_message(payload, message_opts, state)
    Connection.send_message(state.connection, message, state.send_timeout)

    :telemetry.execute(
      [:pulsar_ex, :producer, :send],
      %{count: 1, duration: System.monotonic_time() - start},
      state.metadata
    )

    {:noreply, state}
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

  @impl true
  def terminate(reason, %{topic_name: topic_name} = state) do
    len = :queue.len(state.queue)

    if len > 0 do
      Logger.error(
        "Sending closed error to #{len} remaining message for producer #{state.producer_id} with topic #{
          state.topic_name
        }"
      )

      fast_fail(state.queue, state)
    end

    state = %{state | queue: :queue.new()}

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

      {:shutdown, :close} ->
        Logger.debug(
          "Stopping producer #{state.producer_id} with topic #{topic_name}, #{inspect(reason)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :producer, :close],
          %{count: 1},
          state.metadata
        )

        # avoid immediate recreate on broker
        Process.sleep(state.termination_timeout)
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

        # avoid immediate recreate on broker
        Process.sleep(state.termination_timeout)
        state
    end
  end

  defp fast_fail(queue, state) do
    case :queue.out(queue) do
      {:empty, {[], []}} ->
        nil

      {{:value, {_, from}}, queue} ->
        reply(from, {:error, :closed})
        fast_fail(queue, state)
    end
  end

  defp reply(nil, _), do: nil
  defp reply(from, reply), do: GenServer.reply(from, reply)
end
