defmodule PulsarEx.Producer do
  @enforce_keys [
    :state,
    :cluster,
    :topic,
    :producer_id,
    :producer_opts,
    :compression,
    :batch_enabled,
    :batch_size,
    :max_queue_size,
    :max_message_size,
    :flush_interval,
    :max_redirects,
    :max_attempts,
    :redirects,
    :attempts,
    :backoff,
    :pending_sends,
    :queue,
    :batch,
    :queue_size,
    :flush_timer,
    :authoritative
  ]

  defstruct [
    :state,
    :cluster,
    :topic,
    :producer_id,
    :producer_opts,
    :compression,
    :batch_enabled,
    :batch_size,
    :max_queue_size,
    :max_message_size,
    :flush_interval,
    :max_redirects,
    :max_attempts,
    :redirects,
    :attempts,
    :backoff,
    :pending_sends,
    :queue,
    :batch,
    :queue_size,
    :flush_timer,
    :authoritative,
    :broker_url,
    :connection,
    :connection_monitor,
    :producer_name,
    :last_sequence_id,
    :producer_access_mode,
    :properties,
    :error
  ]

  use GenServer

  alias PulsarEx.Application, as: App

  alias PulsarEx.{
    Cluster,
    Broker,
    Topic,
    Message,
    ProducerMessage,
    Backoff,
    ConnectionManager,
    ProducerIDRegistry,
    Connection
  }

  require Logger

  @compression :none
  @batch_enabled false
  @batch_size 100

  @max_queue_size 100
  # 1MB
  @max_message_size 1_048_576
  @flush_interval 100

  @max_redirects 20
  @max_attempts 5

  @backoff_type :rand_exp
  @backoff_min 1_000
  @backoff_max 10_000
  @backoff Backoff.new(
             backoff_type: @backoff_type,
             backoff_min: @backoff_min,
             backoff_max: @backoff_max
           )

  @send_timeout 15_000
  @lookup_timeout 10_000
  @connect_timeout 10_000
  @tick_interval 1000

  def start_link(
        {%Cluster{cluster_name: cluster_name} = cluster, %Topic{} = topic, producer_opts}
      ) do
    producer_id = App.get_producer_id(cluster_name)

    GenServer.start_link(__MODULE__, {cluster, topic, producer_id, producer_opts},
      name:
        {:via, Registry,
         {ProducerIDRegistry, {cluster_name, producer_id}, {cluster, topic, producer_opts}}}
    )
  end

  def produce(producer, message_or_messages) do
    produce(producer, message_or_messages, System.monotonic_time(:millisecond) + @send_timeout)
  end

  def produce(producer, %Message{} = message, deadline) do
    timeout = 2 * (deadline - System.monotonic_time(:millisecond))
    timeout = max(timeout, @send_timeout)
    GenServer.call(producer, {:produce, message, deadline}, timeout)
  end

  def produce(producer, [%Message{} | _] = messages, deadline) do
    timeout = 2 * (deadline - System.monotonic_time(:millisecond))
    timeout = max(timeout, @send_timeout)
    GenServer.call(producer, {:produce, messages, deadline}, timeout)
  end

  def close(producer) do
    GenServer.cast(producer, :close)
  end

  def send_response(producer, response) do
    GenServer.cast(producer, {:send_response, response})
  end

  @impl true
  def init({%Cluster{cluster_opts: cluster_opts} = cluster, topic, producer_id, producer_opts}) do
    Logger.metadata(
      cluster: "#{cluster}",
      broker: nil,
      topic: "#{topic}",
      producer_id: producer_id
    )

    Process.flag(:trap_exit, true)

    producer_opts =
      cluster_opts
      |> Keyword.get(:producer_opts, [])
      |> Keyword.merge(producer_opts, fn
        :properties, v1, v2 -> Keyword.merge(v1, v2)
        _, _, v -> v
      end)

    compression = Keyword.get(producer_opts, :compression, @compression)
    batch_enabled = Keyword.get(producer_opts, :batch_enabled, @batch_enabled)
    batch_size = max(Keyword.get(producer_opts, :batch_size, @batch_size), 1)

    max_queue_size = min(Keyword.get(producer_opts, :max_queue_size, @max_queue_size), 10000)

    max_message_size =
      min(Keyword.get(producer_opts, :max_message_size, @max_message_size), 52_428_800)

    flush_interval = max(Keyword.get(producer_opts, :flush_interval, @flush_interval), 10)

    max_redirects = Keyword.get(producer_opts, :max_redirects, @max_redirects)
    max_attempts = Keyword.get(producer_opts, :max_attempts, @max_attempts)

    Logger.debug("Starting producer")

    Process.send(self(), :connect, [])
    flush_timer = Process.send_after(self(), :flush, flush_interval)
    Process.send_after(self(), :tick, @tick_interval)

    state = %__MODULE__{
      state: :LOOKUP,
      cluster: cluster,
      topic: topic,
      producer_id: producer_id,
      producer_opts: producer_opts,
      compression: compression,
      batch_enabled: batch_enabled,
      batch_size: batch_size,
      max_queue_size: max_queue_size,
      max_message_size: max_message_size,
      flush_interval: flush_interval,
      max_redirects: max_redirects,
      max_attempts: max_attempts,
      redirects: 0,
      attempts: 0,
      backoff: @backoff,
      pending_sends: %{},
      queue: :queue.new(),
      batch: [],
      queue_size: 0,
      flush_timer: flush_timer,
      authoritative: false
    }

    {:ok, state}
  end

  # ===================  handle_info  ===================
  @impl true
  def handle_info(:connect, %{max_attempts: max_attempts, attempts: attempts, error: err} = state)
      when attempts >= max_attempts do
    Logger.error("Exhausted max attempts, #{inspect(err)}")

    :telemetry.execute(
      [:pulsar_ex, :producer, :max_attempts],
      %{count: 1},
      state
    )

    {:stop, {:shutdown, err}, state}
  end

  def handle_info(:connect, %{max_redirects: max_redirects, redirects: redirects} = state)
      when redirects >= max_redirects do
    Logger.error("Exhausted max redirects")

    :telemetry.execute(
      [:pulsar_ex, :producer, :max_redirects],
      %{count: 1},
      state
    )

    {:stop, {:shutdown, :too_many_redirects}, state}
  end

  def handle_info(
        :connect,
        %{state: :LOOKUP, cluster: %Cluster{brokers: brokers, port: port}, broker_url: nil} =
          state
      ) do
    broker_url = %Broker{host: Enum.random(brokers), port: port} |> to_string()
    handle_info(:connect, %{state | broker_url: broker_url})
  end

  def handle_info(
        :connect,
        %{
          state: :LOOKUP,
          cluster: %Cluster{cluster_name: cluster_name} = cluster,
          topic: topic,
          producer_id: producer_id,
          authoritative: authoritative,
          broker_url: broker_url,
          redirects: redirects,
          attempts: attempts,
          backoff: backoff
        } = state
      ) do
    Logger.metadata(
      cluster: "#{cluster}",
      broker: broker_url,
      topic: "#{topic}",
      producer_id: producer_id
    )

    deadline = System.monotonic_time(:millisecond) + @lookup_timeout

    with {:ok, conn} <- ConnectionManager.get_connection(cluster_name, broker_url),
         {:connect, broker_url} <-
           connection_module().lookup_topic(conn, to_string(topic), authoritative, deadline) do
      Logger.debug("Successfully looked up topic")

      :telemetry.execute(
        [:pulsar_ex, :producer, :lookup, :success],
        %{count: 1},
        state
      )

      Process.send(self(), :connect, [])

      {:noreply, %{state | state: :CONNECT, broker_url: broker_url, redirects: 0}}
    else
      {:redirect, broker_url, authoritative} ->
        Logger.debug("Redirect topic to broker [#{broker_url}]")

        :telemetry.execute(
          [:pulsar_ex, :producer, :lookup, :redirects],
          %{count: 1},
          state
        )

        Process.send(self(), :connect, [])

        {:noreply,
         %{state | authoritative: authoritative, broker_url: broker_url, redirects: redirects + 1}}

      {:error, err} ->
        Logger.error("Error looking up topic, #{inspect(err)}")

        :telemetry.execute(
          [:pulsar_ex, :producer, :lookup, :error],
          %{count: 1},
          state
        )

        {wait, backoff} = Backoff.backoff(backoff)
        Process.send_after(self(), :connect, wait)

        {:noreply,
         %{
           state
           | authoritative: false,
             broker_url: nil,
             redirects: 0,
             attempts: attempts + 1,
             backoff: backoff,
             error: err
         }}
    end
  end

  def handle_info(
        :connect,
        %{
          state: :CONNECT,
          cluster: %Cluster{cluster_name: cluster_name},
          topic: topic,
          producer_id: producer_id,
          producer_opts: producer_opts,
          attempts: attempts,
          backoff: backoff,
          broker_url: broker_url
        } = state
      ) do
    deadline = System.monotonic_time(:millisecond) + @connect_timeout

    with {:ok, conn} <- ConnectionManager.get_connection(cluster_name, broker_url),
         {:ok,
          %{
            producer_name: producer_name,
            last_sequence_id: last_sequence_id,
            max_message_size: max_message_size,
            producer_access_mode: producer_access_mode,
            properties: properties
          }} <-
           connection_module().create_producer(
             conn,
             producer_id,
             to_string(topic),
             producer_opts,
             deadline
           ) do
      Logger.debug("Successfully created producer")

      :telemetry.execute(
        [:pulsar_ex, :producer, :connect, :success],
        %{count: 1},
        state
      )

      ref = Process.monitor(conn)

      {:noreply,
       %{
         state
         | state: :READY,
           redirects: 0,
           attempts: 0,
           backoff: @backoff,
           error: nil,
           connection: conn,
           connection_monitor: ref,
           producer_name: producer_name,
           last_sequence_id: last_sequence_id,
           max_message_size: max_message_size,
           producer_access_mode: producer_access_mode,
           properties: properties
       }}
    else
      {:error, err} ->
        Logger.error("Error creating producer, #{inspect(err)}")

        :telemetry.execute(
          [:pulsar_ex, :producer, :connect, :error],
          %{count: 1},
          state
        )

        {wait, backoff} = Backoff.backoff(backoff)
        Process.send_after(self(), :connect, wait)

        {:noreply,
         %{
           state
           | state: :LOOKUP,
             authoritative: false,
             broker_url: nil,
             connection: nil,
             redirects: 0,
             attempts: attempts + 1,
             backoff: backoff,
             error: err
         }}
    end
  end

  def handle_info(
        :tick,
        %{pending_sends: pending_sends, queue: queue, batch: batch, queue_size: queue_size} =
          state
      ) do
    {pending_sends, pending_timeouts} = tick_pending_sends(pending_sends, state)
    {queue, queue_timeouts} = tick_queue(queue, state)
    {batch, batch_timeouts} = tick_batch(batch, state)

    timeouts = pending_timeouts + queue_timeouts + batch_timeouts
    queue_size = queue_size - timeouts

    :telemetry.execute(
      [:pulsar_ex, :producer, :pending_sends, :timeout],
      %{count: timeouts},
      state
    )

    :telemetry.execute(
      [:pulsar_ex, :producer, :queue],
      %{size: queue_size},
      state
    )

    :telemetry.execute(
      [:pulsar_ex, :producer, :pending_sends],
      %{size: Enum.count(pending_sends)},
      state
    )

    Process.send_after(self(), :tick, @tick_interval)

    {:noreply,
     %{state | pending_sends: pending_sends, queue: queue, batch: batch, queue_size: queue_size}}
  end

  def handle_info(:flush, %{flush_interval: flush_interval, flush_timer: flush_timer} = state) do
    Process.cancel_timer(flush_timer)

    case flush(state) do
      {:ok, :cont, state} ->
        Process.send(self(), :flush, [])
        {:noreply, state}

      {:ok, :halt, state} ->
        flush_timer = Process.send_after(self(), :flush, flush_interval)
        {:noreply, %{state | flush_timer: flush_timer}}

      {:error, err, state} ->
        {:stop, {:shutdown, err}, state}
    end
  end

  def handle_info({:DOWN, _, _, _, _}, state) do
    Logger.error("Connection down")

    :telemetry.execute(
      [:pulsar_ex, :producer, :connection_down],
      %{count: 1},
      state
    )

    {:stop, {:shutdown, :connection_down}, state}
  end

  # ===================  handle_call  ===================
  @impl true
  def handle_call(
        {:produce, %Message{}, _deadline},
        _from,
        %{max_queue_size: max_queue_size, queue_size: queue_size} = state
      )
      when queue_size + 1 > max_queue_size do
    {:reply, {:error, :overflow}, state}
  end

  def handle_call(
        {:produce, %Message{payload: payload}, _deadline},
        _from,
        %{max_message_size: max_message_size} = state
      )
      when byte_size(payload) > max_message_size do
    {:reply, {:error, :message_size_too_large}, state}
  end

  def handle_call(
        {:produce, %Message{deliver_at_time: nil} = message, deadline},
        from,
        %{batch_enabled: true, batch: batch, batch_size: batch_size, queue_size: queue_size} =
          state
      )
      when length(batch) + 1 < batch_size do
    {:noreply, %{state | batch: [{message, from, deadline} | batch], queue_size: queue_size + 1}}
  end

  def handle_call(
        {:produce, %Message{deliver_at_time: nil} = message, deadline},
        from,
        %{batch_enabled: true, queue: queue, batch: batch, queue_size: queue_size} = state
      ) do
    batch = [{message, from, deadline} | batch]
    queue = :queue.in(batch, queue)
    Process.send(self(), :flush, [])
    {:noreply, %{state | queue: queue, batch: [], queue_size: queue_size + 1}}
  end

  def handle_call(
        {:produce, %Message{} = message, deadline},
        from,
        %{queue: queue, batch: [], queue_size: queue_size} = state
      ) do
    queue = :queue.in({message, from, deadline}, queue)
    Process.send(self(), :flush, [])
    {:noreply, %{state | queue: queue, queue_size: queue_size + 1}}
  end

  def handle_call(
        {:produce, %Message{} = message, deadline},
        from,
        %{queue: queue, batch: batch, queue_size: queue_size} = state
      ) do
    queue = :queue.in(batch, queue)
    queue = :queue.in({message, from, deadline}, queue)
    Process.send(self(), :flush, [])
    {:noreply, %{state | queue: queue, batch: [], queue_size: queue_size + 1}}
  end

  def handle_call({:produce, [], _deadline}, _from, state) do
    {:reply, {:error, :batch_empty}, state}
  end

  def handle_call(
        {:produce, [%Message{} | _], _deadline},
        _from,
        %{batch_enabled: false} = state
      ) do
    {:reply, {:error, :batch_not_enabled}, state}
  end

  def handle_call(
        {:produce, [%Message{} | _] = messages, _deadline},
        _from,
        %{batch_size: batch_size} = state
      )
      when length(messages) > batch_size do
    {:reply, {:error, :batch_size_too_large}, state}
  end

  def handle_call(
        {:produce, [%Message{} | _] = messages, _deadline},
        _from,
        %{queue_size: queue_size, max_queue_size: max_queue_size} = state
      )
      when length(messages) + queue_size > max_queue_size do
    {:reply, {:error, :overflow}, state}
  end

  def handle_call(
        {:produce, [%Message{} | _] = messages, deadline},
        from,
        %{queue: queue, batch: batch, max_message_size: max_message_size, queue_size: queue_size} =
          state
      ) do
    cond do
      Enum.reduce(messages, nil, &(&1.deliver_at_time || &2)) != nil ->
        {:reply, {:error, :message_delayed_in_batch}, state}

      Enum.any?(messages, &(byte_size(&1.payload) > max_message_size)) ->
        {:reply, {:error, :message_size_too_large}, state}

      batch == [] ->
        queue = :queue.in({messages, from, deadline}, queue)
        Process.send(self(), :flush, [])
        {:noreply, %{state | queue: queue, queue_size: queue_size + length(messages)}}

      true ->
        queue = :queue.in(batch, queue)
        queue = :queue.in({messages, from, deadline}, queue)
        Process.send(self(), :flush, [])
        {:noreply, %{state | queue: queue, batch: [], queue_size: queue_size + length(messages)}}
    end
  end

  # ===================  handle_cast  ===================
  @impl true
  def handle_cast(:close, state) do
    Logger.warn("Received close command")

    :telemetry.execute(
      [:pulsar_ex, :producer, :closed],
      %{count: 1},
      state
    )

    {:stop, {:shutdown, :closed}, state}
  end

  def handle_cast(
        {:send_response, {:ok, %{sequence_id: sequence_id, message_id: message_id}}},
        %{pending_sends: pending_sends, queue_size: queue_size} = state
      ) do
    case Map.pop(pending_sends, sequence_id) do
      {nil, _} ->
        Logger.warn("Received unknown send response, messages might have timed out")

        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :response, :missing],
          %{count: 1},
          state
        )

        {:noreply, state}

      {{sent, send_ts}, pending_sends} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.debug("Received successful send response after #{duration_ms}ms")

        count = reply(sent, {:ok, message_id})

        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :response, :success],
          %{count: count, duration: duration},
          state
        )

        {:noreply, %{state | pending_sends: pending_sends, queue_size: queue_size - count}}
    end
  end

  def handle_cast(
        {:send_response, {:error, %{sequence_id: sequence_id, error: err}}},
        %{pending_sends: pending_sends, queue_size: queue_size} = state
      ) do
    case Map.pop(pending_sends, sequence_id) do
      {nil, _} ->
        Logger.error(
          "Received unknown send response, messages might have timed out, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :response, :missing],
          %{count: 1},
          state
        )

        {:noreply, state}

      {{sent, send_ts}, pending_sends} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.error("Received error send response after #{duration_ms}ms, #{inspect(err)}")

        count = reply(sent, {:error, err})

        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :response, :error],
          %{count: count, duration: duration},
          state
        )

        {:noreply, %{state | pending_sends: pending_sends, queue_size: queue_size - count}}
    end
  end

  # ===================  terminate  ===================
  @impl true
  def terminate(:normal, %{producer_id: producer_id, connection: conn} = state) do
    Logger.debug("Stopping producer")

    flush_state(state, :terminated)

    if conn do
      Connection.close_producer(conn, producer_id)
    end

    state
  end

  def terminate(:shutdown, %{producer_id: producer_id, connection: conn} = state) do
    Logger.debug("Stopping producer")

    flush_state(state, :terminated)

    if conn do
      Connection.close_producer(conn, producer_id)
    end

    state
  end

  def terminate({:shutdown, err}, %{producer_id: producer_id, connection: conn} = state) do
    Logger.warn("Stopping producer, #{inspect(err)}")

    flush_state(state, err)

    if conn do
      Connection.close_producer(conn, producer_id)
    end

    state
  end

  def terminate(reason, %{producer_id: producer_id, connection: conn} = state) do
    Logger.error("Exiting producer, #{inspect(reason)}")

    flush_state(state, :exit)

    if conn do
      Connection.close_producer(conn, producer_id)
    end

    :telemetry.execute(
      [:pulsar_ex, :producer, :exit],
      %{count: 1},
      state
    )

    state
  end

  # ===================  private  ===================
  defp tick_pending_sends(pending_sends, state) do
    Enum.reduce(pending_sends, {%{}, 0}, fn
      {seq_id, {{%Message{}, _from, deadline} = sent, send_ts}}, {acc, timeouts} ->
        if System.monotonic_time(:millisecond) >= deadline do
          reply_error(sent, :timeout, state)
          {acc, timeouts + 1}
        else
          {Map.put(acc, seq_id, {sent, send_ts}), timeouts}
        end

      {seq_id, {{[%Message{} | _] = msgs, _from, deadline} = sent, send_ts}}, {acc, timeouts} ->
        if System.monotonic_time(:millisecond) >= deadline do
          reply_error(sent, :timeout, state)
          {acc, timeouts + length(msgs)}
        else
          {Map.put(acc, seq_id, {sent, send_ts}), timeouts}
        end

      {seq_id, {batch, send_ts}}, {acc, timeouts} ->
        {batch, batch_timeouts} = tick_batch(batch, state)

        if batch == [] do
          {acc, timeouts + batch_timeouts}
        else
          {Map.put(acc, seq_id, {batch, send_ts}), timeouts + batch_timeouts}
        end
    end)
  end

  defp tick_queue(queue, state, acc \\ {:queue.new(), 0})

  defp tick_queue(queue, state, {acc, timeouts}) do
    case :queue.out(queue) do
      {:empty, _} ->
        {acc, timeouts}

      {{:value, {%Message{}, _from, deadline} = message}, queue} ->
        if System.monotonic_time(:millisecond) >= deadline do
          reply_error(message, :timeout, state)
          tick_queue(queue, state, {acc, timeouts + 1})
        else
          tick_queue(queue, state, {:queue.in(message, acc), timeouts})
        end

      {{:value, {[%Message{} | _] = msgs, _from, deadline} = messages}, queue} ->
        if System.monotonic_time(:millisecond) >= deadline do
          reply_error(messages, :timeout, state)
          tick_queue(queue, state, {acc, timeouts + length(msgs)})
        else
          tick_queue(queue, state, {:queue.in(messages, acc), timeouts})
        end

      {{:value, batch}, queue} ->
        {batch, batch_timeouts} = tick_batch(batch, state)

        if batch == [] do
          tick_queue(queue, state, {acc, timeouts + batch_timeouts})
        else
          tick_queue(queue, state, {:queue.in(batch, acc), timeouts + batch_timeouts})
        end
    end
  end

  defp tick_batch(batch, state) do
    {batch, timeouts} =
      Enum.reduce(batch, {[], 0}, fn {%Message{}, _from, deadline} = msg, {acc, timeouts} ->
        if System.monotonic_time(:millisecond) >= deadline do
          reply_error(msg, :timeout, state)
          {acc, timeouts + 1}
        else
          {[msg | acc], timeouts}
        end
      end)

    {Enum.reverse(batch), timeouts}
  end

  defp produce_message(
         %Message{
           payload: payload,
           properties: properties,
           partition_key: partition_key,
           ordering_key: ordering_key,
           event_time: event_time,
           deliver_at_time: deliver_at_time
         },
         %{
           topic: topic,
           producer_id: producer_id,
           producer_name: producer_name,
           compression: compression,
           last_sequence_id: last_sequence_id
         } = state
       ) do
    sequence_id = last_sequence_id + 1

    message = %ProducerMessage{
      topic: topic,
      producer_id: producer_id,
      producer_name: producer_name,
      sequence_id: sequence_id,
      payload: payload,
      properties: properties,
      partition_key: partition_key,
      ordering_key: ordering_key,
      event_time: event_time,
      deliver_at_time: deliver_at_time,
      compression: compression
    }

    {message, %{state | last_sequence_id: sequence_id}}
  end

  defp produce_messages([%Message{} | _] = messages, state) do
    {messages, state} =
      Enum.reduce(messages, {[], state}, fn message, {msgs, acc} ->
        {msg, acc} = produce_message(message, acc)
        {[msg | msgs], acc}
      end)

    {Enum.reverse(messages), state}
  end

  defp produce_messages([{%Message{}, _from, _deadline} | _] = batch, state) do
    {messages, state} =
      Enum.reduce(Enum.reverse(batch), {[], state}, fn {message, _from, _deadline}, {msgs, acc} ->
        {msg, acc} = produce_message(message, acc)
        {[msg | msgs], acc}
      end)

    {Enum.reverse(messages), state}
  end

  defp flush(
         %{
           state: :READY,
           producer_id: producer_id,
           connection: conn,
           queue: queue,
           pending_sends: pending_sends
         } = state
       ) do
    case :queue.out(queue) do
      {:empty, _} ->
        flush_batch(state)

      {{:value, {%Message{} = message, from, deadline}}, queue} ->
        {%{sequence_id: sequence_id} = producer_message, state} = produce_message(message, state)

        case connection_module().send_message(
               conn,
               producer_id,
               sequence_id,
               producer_message,
               deadline
             ) do
          :ok ->
            Logger.debug("Successfully sent 1 message")

            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :request, :success],
              %{count: 1},
              state
            )

            pending_sends =
              Map.put(
                pending_sends,
                sequence_id,
                {{message, from, deadline}, System.monotonic_time()}
              )

            {:ok, :cont, %{state | queue: queue, pending_sends: pending_sends}}

          {:error, err} ->
            Logger.error("Error sending 1 message, #{inspect(err)}")

            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :request, :error],
              %{count: 1},
              state
            )

            {:error, err, state}
        end

      {{:value, {[%Message{} | _] = messages, from, deadline}}, queue} ->
        {[%{sequence_id: sequence_id} | _] = producer_messages, state} =
          produce_messages(messages, state)

        case connection_module().send_messages(
               conn,
               producer_id,
               sequence_id,
               producer_messages,
               deadline
             ) do
          :ok ->
            Logger.debug("Successfully sent #{length(messages)} message")

            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :request, :success],
              %{count: length(messages)},
              state
            )

            pending_sends =
              Map.put(
                pending_sends,
                sequence_id,
                {{messages, from, deadline}, System.monotonic_time()}
              )

            {:ok, :cont, %{state | queue: queue, pending_sends: pending_sends}}

          {:error, err} ->
            Logger.error("Error sending #{length(messages)} message, #{inspect(err)}")

            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :request, :error],
              %{count: length(messages)},
              state
            )

            {:error, err, state}
        end

      {{:value, batch}, queue} ->
        {[%{sequence_id: sequence_id} | _] = producer_messages, state} =
          produce_messages(batch, state)

        {_, _, deadline} = Enum.min_by(batch, fn {_message, _from, deadline} -> deadline end)

        case connection_module().send_messages(
               conn,
               producer_id,
               sequence_id,
               producer_messages,
               deadline
             ) do
          :ok ->
            Logger.debug("Successfully sent #{length(batch)} message")

            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :request, :success],
              %{count: length(batch)},
              state
            )

            pending_sends = Map.put(pending_sends, sequence_id, {batch, System.monotonic_time()})
            {:ok, :cont, %{state | queue: queue, pending_sends: pending_sends}}

          {:error, err} ->
            Logger.error("Error sending #{length(batch)} message, #{inspect(err)}")

            :telemetry.execute(
              [:pulsar_ex, :producer, :send, :request, :error],
              %{count: length(batch)},
              state
            )

            {:error, err, state}
        end
    end
  end

  defp flush(state), do: {:ok, :halt, state}

  defp flush_batch(%{batch: []} = state), do: {:ok, :halt, state}

  defp flush_batch(
         %{producer_id: producer_id, connection: conn, batch: batch, pending_sends: pending_sends} =
           state
       ) do
    {[%{sequence_id: sequence_id} | _] = producer_messsages, state} =
      produce_messages(batch, state)

    {_, _, deadline} = Enum.min_by(batch, fn {_message, _from, deadline} -> deadline end)

    case connection_module().send_messages(
           conn,
           producer_id,
           sequence_id,
           producer_messsages,
           deadline
         ) do
      :ok ->
        Logger.debug("Successfully sent #{length(batch)} message")

        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :request, :success],
          %{count: length(batch)},
          state
        )

        pending_sends = Map.put(pending_sends, sequence_id, {batch, System.monotonic_time()})
        {:ok, :halt, %{state | batch: [], pending_sends: pending_sends}}

      {:error, err} ->
        Logger.error("Error sending #{length(batch)} message, #{inspect(err)}")

        :telemetry.execute(
          [:pulsar_ex, :producer, :send, :request, :error],
          %{count: length(batch)},
          state
        )

        {:error, err, state}
    end
  end

  defp reply({%Message{}, from, _deadline}, response) do
    GenServer.reply(from, response)
    1
  end

  defp reply({[%Message{} | _] = msgs, from, _deadline}, response) do
    GenServer.reply(from, response)
    length(msgs)
  end

  defp reply(batch, response) when is_list(batch) do
    Enum.each(batch, &reply(&1, response))
    length(batch)
  end

  defp reply_error({%Message{}, from, _deadline}, err, state) do
    stats_error(1, err, state)
    GenServer.reply(from, {:error, err})
  end

  defp reply_error({[%Message{} | _] = msgs, from, _deadline}, err, state) do
    stats_error(length(msgs), err, state)
    GenServer.reply(from, {:error, err})
  end

  defp reply_error(batch, err, state) when is_list(batch) do
    Enum.each(batch, &reply_error(&1, err, state))
  end

  defp reply_pending_sends(pending_sends, err, state) do
    Enum.each(pending_sends, fn {_seq_id, {sent, _send_ts}} ->
      reply_error(sent, err, state)
    end)
  end

  defp reply_queue(queue, err, state) do
    case :queue.out(queue) do
      {:empty, _} ->
        nil

      {{:value, q}, queue} ->
        reply_error(q, err, state)
        reply_queue(queue, err, state)
    end
  end

  defp reply_batch(batch, err, state) do
    reply_error(batch, err, state)
  end

  defp flush_state(%{pending_sends: pending_sends, queue: queue, batch: batch} = state, err) do
    reply_pending_sends(pending_sends, err, state)
    reply_queue(queue, err, state)
    reply_batch(batch, err, state)
  end

  defp stats_error(count, err, state)
       when err in [
              :timeout,
              :closed,
              :terminated,
              :exit,
              :connection_down,
              :too_many_redirects,
              :connection_not_ready
            ] do
    :telemetry.execute(
      [:pulsar_ex, :producer, :send, :error, err],
      %{count: count},
      state
    )
  end

  defp stats_error(count, _err, state) do
    :telemetry.execute(
      [:pulsar_ex, :producer, :send, :error, :unknown],
      %{count: count},
      state
    )
  end

  defp connection_module() do
    Application.get_env(:pulsar_ex, :connection_module, Connection)
  end
end
