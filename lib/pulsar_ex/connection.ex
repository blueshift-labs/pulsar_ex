defmodule PulsarEx.Connection do
  defmodule State do
    @enforce_keys [
      :broker,
      :broker_name,
      :last_request_id,
      :last_producer_id,
      :last_consumer_id,
      :requests,
      :producers,
      :consumers,
      :buffer,
      :metadata
    ]

    defstruct [
      :broker,
      :broker_name,
      :last_request_id,
      :last_producer_id,
      :last_consumer_id,
      :requests,
      :producers,
      :consumers,
      :buffer,
      :metadata,
      :socket,
      :max_message_size,
      :last_server_ts
    ]
  end

  use Connection

  import PulsarEx.IO

  require Logger

  alias PulsarEx.{Broker, ProducerMessage}

  alias PulsarEx.Proto.{
    CommandConnect,
    CommandConnected,
    CommandPing,
    CommandPong,
    CommandProducer,
    CommandProducerSuccess,
    CommandCloseProducer,
    CommandSuccess,
    CommandError,
    CommandCloseConsumer,
    CommandSendReceipt,
    CommandSendError,
    CommandSubscribe,
    CommandFlow,
    CommandAck,
    CommandAckResponse,
    CommandRedeliverUnacknowledgedMessages,
    CommandMessage,
    MessageIdData
  }

  @client_version "PulsarEx #{Mix.Project.config()[:version]}"
  @protocol_version 13

  @connection_timeout 5000
  @ping_interval 45_000

  def create_producer(conn, topic, opts \\ []) do
    GenServer.call(conn, {:create_producer, topic, opts})
  end

  def subscribe(conn, topic, subscription, sub_type, opts \\ []) do
    GenServer.call(conn, {:subscribe, topic, subscription, sub_type, opts})
  end

  def send_message(conn, producer_id, sequence_id, %ProducerMessage{} = message, timeout) do
    GenServer.call(conn, {:send, producer_id, sequence_id, message}, timeout)
  end

  def send_messages(conn, producer_id, sequence_id, messages, timeout) when is_list(messages) do
    GenServer.call(conn, {:send, producer_id, sequence_id, messages}, timeout)
  end

  def flow_permits(conn, consumer_id, permits) do
    GenServer.call(conn, {:flow_permits, consumer_id, permits})
  end

  def redeliver(conn, consumer_id, msg_ids) do
    GenServer.call(conn, {:redeliver, consumer_id, msg_ids})
  end

  def ack(conn, consumer_id, ack_type, msg_ids) do
    GenServer.call(conn, {:ack, consumer_id, ack_type, msg_ids})
  end

  def start_link(%Broker{} = broker) do
    Connection.start_link(__MODULE__, broker)
  end

  @impl true
  def init(broker) do
    Logger.debug("Starting connection to broker #{Broker.to_name(broker)}")

    Process.flag(:trap_exit, true)

    state = %State{
      broker: broker,
      broker_name: Broker.to_name(broker),
      last_request_id: -1,
      last_producer_id: -1,
      last_consumer_id: -1,
      requests: %{},
      producers: %{},
      consumers: %{},
      buffer: <<>>,
      metadata: %{broker: Broker.to_name(broker)}
    }

    {:connect, :init, state}
  end

  @impl true
  def connect(:init, %{broker: broker} = state) do
    with {:ok, socket} <- do_connect(broker.host, broker.port),
         {:ok, max_message_size} <- do_handshake(socket) do
      :inet.setopts(socket, active: :once)
      Process.send_after(self(), :send_ping, @ping_interval)
      Logger.debug("Connection established to broker #{state.broker_name}")

      :telemetry.execute(
        [:pulsar_ex, :connection, :success],
        %{count: 1},
        state.metadata
      )

      {:ok,
       %{
         state
         | socket: socket,
           last_server_ts: System.monotonic_time(:millisecond),
           max_message_size: max_message_size
       }}
    else
      {:error, _} = err ->
        :telemetry.execute(
          [:pulsar_ex, :connection, :error],
          %{count: 1},
          state.metadata
        )

        {:stop, err, state}
    end
  end

  @impl true
  def disconnect(err, state) do
    Logger.error("Disconnecting from broker #{state.broker_name}, #{inspect(err)}")

    :gen_tcp.close(state.socket)

    {:stop, err, state}
  end

  @impl true
  def terminate(reason, state) do
    case reason do
      :normal ->
        Logger.debug("Closing connection from broker #{state.broker_name}}, #{inspect(reason)}")

      :shutdown ->
        Logger.debug("Closing connection from broker #{state.broker_name}}, #{inspect(reason)}")

      {:shutdown, _} ->
        Logger.debug("Closing connection from broker #{state.broker_name}}, #{inspect(reason)}")

      _ ->
        Logger.error("Closing connection from broker #{state.broker_name}}, #{inspect(reason)}")

        :telemetry.execute(
          [:pulsar_ex, :connection, :exit],
          %{count: 1},
          state.metadata
        )
    end

    state
  end

  defp do_connect(host, port) do
    socket_opts = Application.get_env(:pulsar_ex, :socket_opts, []) |> optimize_socket_opts()
    connection_timeout = Application.get_env(:pulsar_ex, :connection_timeout, @connection_timeout)
    :gen_tcp.connect(to_charlist(host), port, socket_opts, connection_timeout)
  end

  defp do_handshake(socket) do
    command =
      CommandConnect.new(
        client_version: @client_version,
        protocol_version: @protocol_version
      )

    with :ok <- :gen_tcp.send(socket, encode_command(command)),
         {:ok, data} <- :gen_tcp.recv(socket, 0),
         {[{%CommandConnected{} = connected, _}], _} <- decode(data) do
      {:ok, connected.max_message_size}
    else
      _ ->
        {:error, :handshake}
    end
  end

  defp optimize_socket_opts(socket_opts) do
    socket_opts =
      socket_opts
      |> Enum.reject(fn
        :binary -> true
        {:nodelay, _} -> true
        {:active, _} -> true
        {:keepalive, _} -> true
      end)

    [:binary, nodelay: true, active: false, keepalive: true] ++ socket_opts
  end

  # ================== handle_call! =====================
  @impl true
  def handle_call({:create_producer, topic, opts}, from, state) do
    Logger.debug("Creating producer on broker #{state.broker_name}")

    request =
      CommandProducer.new(
        request_id: state.last_request_id + 1,
        producer_id: state.last_producer_id + 1,
        topic: topic
      )

    state = %{state | last_request_id: request.request_id, last_producer_id: request.producer_id}

    request =
      case Keyword.get(opts, :producer_name) do
        nil ->
          request

        producer_name ->
          %{request | producer_name: producer_name, user_provided_producer_name: true}
      end

    request =
      case Keyword.get(opts, :producer_access_mode) do
        nil ->
          request

        mode ->
          %{request | producer_access_mode: producer_access_mode(mode)}
      end

    request = %{request | metadata: Keyword.get(opts, :properties) |> to_kv()}

    case :gen_tcp.send(state.socket, encode_command(request)) do
      :ok ->
        requests =
          Map.put(
            state.requests,
            {:request_id, request.request_id},
            {from, System.monotonic_time(:millisecond), request}
          )

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
        :telemetry.execute(
          [:pulsar_ex, :connection, :create_producer, :error],
          %{count: 1},
          state.metadata
        )

        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call({:subscribe, topic, subscription, sub_type, opts}, from, state) do
    Logger.debug(
      "Subscribing consumer to topic #{topic} with subscription #{subscription} in #{sub_type} mode, on broker #{
        state.broker_name
      }"
    )

    request =
      CommandSubscribe.new(
        request_id: state.last_request_id + 1,
        consumer_id: state.last_consumer_id + 1,
        topic: topic,
        subscription: subscription,
        subType: subscription_type(sub_type)
      )

    state = %{state | last_request_id: request.request_id, last_consumer_id: request.consumer_id}

    request =
      case Keyword.get(opts, :consumer_name) do
        nil -> request
        consumer_name -> %{request | consumer_name: consumer_name}
      end

    request =
      case Keyword.get(opts, :priority_level) do
        nil -> request
        priority_level -> %{request | priority_level: priority_level}
      end

    request =
      case Keyword.get(opts, :durable) do
        nil -> request
        durable -> %{request | durable: durable}
      end

    request =
      case Keyword.get(opts, :read_compacted) do
        nil -> request
        read_compacted -> %{request | read_compacted: read_compacted}
      end

    request =
      case Keyword.get(opts, :force_topic_creation) do
        nil -> request
        force_topic_creation -> %{request | force_topic_creation: force_topic_creation}
      end

    request =
      case Keyword.get(opts, :initial_position) do
        nil -> request
        init_position -> %{request | initialPosition: initial_position(init_position)}
      end

    request = %{request | metadata: Keyword.get(opts, :properties) |> to_kv()}

    case :gen_tcp.send(state.socket, encode_command(request)) do
      :ok ->
        requests =
          Map.put(
            state.requests,
            {:request_id, request.request_id},
            {from, System.monotonic_time(:millisecond), request}
          )

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
        :telemetry.execute(
          [:pulsar_ex, :connection, :subscribe, :error],
          %{count: 1},
          state.metadata
        )

        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call({:flow_permits, consumer_id, permits}, _from, state) do
    Logger.debug(
      "Sending Flow with #{permits} permits to broker #{state.broker_name} for consumer #{
        consumer_id
      }"
    )

    command =
      CommandFlow.new(
        consumer_id: consumer_id,
        messagePermits: permits
      )

    case :gen_tcp.send(state.socket, encode_command(command)) do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :connection, :flow_permits, :success],
          %{count: 1},
          state.metadata
        )

        {:reply, :ok, state}

      {:error, _} = err ->
        :telemetry.execute(
          [:pulsar_ex, :connection, :flow_permits, :error],
          %{count: 1},
          state.metadata
        )

        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call({:redeliver, consumer_id, msg_ids}, _from, state) when is_list(msg_ids) do
    Logger.debug(
      "Sending #{length(msg_ids)} redeliver to broker #{state.broker_name} for consumer #{
        consumer_id
      }"
    )

    message_ids =
      Enum.map(msg_ids, fn {ledgerId, entryId} ->
        MessageIdData.new(ledgerId: ledgerId, entryId: entryId)
      end)

    command =
      CommandRedeliverUnacknowledgedMessages.new(
        consumer_id: consumer_id,
        message_ids: message_ids
      )

    case :gen_tcp.send(state.socket, encode_command(command)) do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :connection, :redeliver, :success],
          %{count: 1},
          state.metadata
        )

        {:reply, :ok, state}

      {:error, _} = err ->
        :telemetry.execute(
          [:pulsar_ex, :connection, :redeliver, :error],
          %{count: 1},
          state.metadata
        )

        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call({:ack, consumer_id, ack_type, msg_ids}, _from, state)
      when is_list(msg_ids) do
    Logger.debug(
      "Sending #{length(msg_ids)} acks to broker #{state.broker_name} for consumer #{consumer_id}"
    )

    message_ids =
      Enum.map(msg_ids, fn {ledgerId, entryId} ->
        MessageIdData.new(ledgerId: ledgerId, entryId: entryId)
      end)

    request =
      CommandAck.new(
        request_id: state.last_request_id + 1,
        consumer_id: consumer_id,
        ack_type: ack_type(ack_type),
        message_id: message_ids,
        txnid_least_bits: nil,
        txnid_most_bits: nil
      )

    state = %{state | last_request_id: request.request_id}

    case :gen_tcp.send(state.socket, encode_command(request)) do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :connection, :ack, :success],
          %{count: 1},
          state.metadata
        )

        requests =
          Map.put(
            state.requests,
            {:request_id, request.request_id},
            {nil, System.monotonic_time(:millisecond), request}
          )

        {:reply, :ok, %{state | requests: requests}}

      {:error, _} = err ->
        :telemetry.execute(
          [:pulsar_ex, :connection, :ack, :error],
          %{count: 1},
          state.metadata
        )

        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call(
        {:send, producer_id, sequence_id, messages},
        from,
        state
      )
      when is_list(messages) do
    Logger.debug(
      "Producing #{length(messages)} messages in batch to broker #{state.broker_name} for producer #{
        producer_id
      }"
    )

    request = encode_messages(messages)

    case :gen_tcp.send(state.socket, request) do
      :ok ->
        requests =
          Map.put(
            state.requests,
            {:sequence_id, producer_id, sequence_id},
            {from, System.monotonic_time(:millisecond), request}
          )

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
        :telemetry.execute(
          [:pulsar_ex, :connection, :send, :error],
          %{count: 1},
          state.metadata
        )

        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call(
        {:send, producer_id, sequence_id, message},
        from,
        state
      ) do
    Logger.debug("Producing message to broker #{state.broker_name} for producer #{producer_id}")

    request = encode_message(message)

    case :gen_tcp.send(state.socket, request) do
      :ok ->
        requests =
          Map.put(
            state.requests,
            {:sequence_id, producer_id, sequence_id},
            {from, System.monotonic_time(:millisecond), request}
          )

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
        :telemetry.execute(
          [:pulsar_ex, :connection, :send, :error],
          %{count: 1},
          state.metadata
        )

        {:disconnect, err, err, state}
    end
  end

  # ================== handle_info! =====================
  @impl true
  def handle_info({:tcp_passive, _}, state), do: {:noreply, state}

  @impl true
  def handle_info({:tcp_closed, _}, state), do: {:disconnect, {:error, :closed}, state}

  @impl true
  def handle_info({:tcp_error, _, reason}, state), do: {:disconnect, {:error, reason}, state}

  @impl true
  def handle_info({:tcp, socket, data}, state) do
    Logger.debug("Receiving data from broker #{state.broker_name}")

    {messages, buffer} = decode(<<state.buffer::binary, data::binary>>)

    # handle tcp messages other than consumer messages
    state =
      messages
      |> Enum.reduce(state, fn
        {command, payload}, acc -> handle_command(command, payload, acc)
      end)

    # now bundle the consumer messages to consumers
    messages
    |> Enum.filter(&match?({%CommandMessage{}, _}, &1))
    |> Enum.reduce(%{}, fn {command, msgs}, acc ->
      Map.merge(acc, %{command.consumer_id => msgs}, fn _, m1, m2 -> m1 ++ m2 end)
    end)
    |> Enum.each(fn {consumer_id, msgs} ->
      Logger.debug(
        "Received #{length(msgs)} messages from broker #{state.broker_name} for consumer #{
          consumer_id
        }"
      )

      case Map.get(state.consumers, consumer_id) do
        {pid, _} ->
          GenServer.cast(pid, {:messages, msgs})

        nil ->
          Logger.error(
            "Received #{length(msgs)} unexpected messages from broker #{state.broker_name} for consumer #{
              consumer_id
            }"
          )
      end
    end)

    :inet.setopts(socket, active: :once)
    {:noreply, %{state | buffer: buffer, last_server_ts: System.monotonic_time(:millisecond)}}
  end

  @impl true
  def handle_info(:send_ping, state) do
    Logger.debug("Sending Ping to broker #{state.broker_name}")

    cond do
      System.monotonic_time(:millisecond) - state.last_server_ts > 2 * @ping_interval ->
        {:disconnect, {:error, :closed}, state}

      true ->
        case :gen_tcp.send(state.socket, encode_command(CommandPing.new())) do
          :ok ->
            Process.send_after(self(), :send_ping, @ping_interval)
            {:noreply, state}

          {:error, _} = err ->
            {:disconnect, err, state}
        end
    end
  end

  @impl true
  def handle_info(:send_pong, state) do
    Logger.debug("Sending Pong to broker #{state.broker_name}")

    case :gen_tcp.send(state.socket, encode_command(CommandPong.new())) do
      :ok -> {:noreply, state}
      {:error, _} = err -> {:disconnect, err, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _, _, pid, _}, state) do
    producer = state.producers |> Enum.find(&match?({_, {^pid, _}}, &1))
    consumer = state.consumers |> Enum.find(&match?({_, {^pid, _}}, &1))

    case {producer, consumer} do
      {{producer_id, {_, ref}}, nil} ->
        Logger.error("Closing producer #{producer_id} on broker #{state.broker_name}")

        Process.demonitor(ref)
        producers = Map.delete(state.producers, producer_id)

        request =
          CommandCloseProducer.new(
            request_id: state.last_request_id + 1,
            producer_id: producer_id
          )

        state = %{state | last_request_id: request.request_id, producers: producers}

        case :gen_tcp.send(state.socket, encode_command(request)) do
          :ok ->
            requests =
              Map.put(
                state.requests,
                {:request_id, request.request_id},
                {nil, System.monotonic_time(:millisecond), request}
              )

            {:noreply, %{state | requests: requests}}

          {:error, _} = err ->
            {:disconnect, err, state}
        end

      {nil, {consumer_id, {_, ref}}} ->
        Logger.error("Stopping consumer #{consumer_id} on broker #{state.broker_name}")

        Process.demonitor(ref)
        consumers = Map.delete(state.consumers, consumer_id)

        request =
          CommandCloseConsumer.new(
            request_id: state.last_request_id + 1,
            consumer_id: consumer_id
          )

        state = %{state | last_request_id: request.request_id, consumers: consumers}

        case :gen_tcp.send(state.socket, encode_command(request)) do
          :ok ->
            requests =
              Map.put(
                state.requests,
                {:request_id, request.request_id},
                {nil, System.monotonic_time(:millisecond), request}
              )

            {:noreply, %{state | requests: requests}}

          {:error, _} = err ->
            {:disconnect, err, state}
        end

      {nil, nil} ->
        Logger.error("Detected unexpected process down on broker #{state.broker_name}")

        {:noreply, state}
    end
  end

  # ================== handle_command! =====================
  defp handle_command(%CommandPing{}, _, state) do
    Logger.debug("Received Ping from broker #{state.broker_name}")

    Process.send(self(), :send_pong, [])
    state
  end

  defp handle_command(%CommandPong{}, _, state) do
    Logger.debug("Received Pong from broker #{state.broker_name}")

    state
  end

  # we don't want to handle consumer message here, we will bundle them to consumers
  defp handle_command(%CommandMessage{}, _, state), do: state

  defp handle_command(%CommandCloseProducer{producer_id: producer_id}, _, state) do
    Logger.warn(
      "Received CloseProducer from broker #{state.broker_name} for producer #{producer_id}"
    )

    {producer, producers} = Map.pop(state.producers, producer_id)

    case producer do
      {pid, ref} ->
        Process.demonitor(ref)
        GenServer.cast(pid, :close)

      nil ->
        Logger.error("Producer #{producer_id} is already terminated")
    end

    %{state | producers: producers}
  end

  defp handle_command(%CommandCloseConsumer{consumer_id: consumer_id}, _, state) do
    Logger.warn(
      "Received CloseConsumer from broker #{state.broker_name} for consumer #{consumer_id}"
    )

    {consumer, consumers} = Map.pop(state.consumers, consumer_id)

    case consumer do
      {pid, ref} ->
        Process.demonitor(ref)
        GenServer.cast(pid, :close)

      nil ->
        Logger.error("Consumer #{consumer_id} is already terminated")
    end

    %{state | consumers: consumers}
  end

  defp handle_command(%CommandProducerSuccess{producer_ready: true} = response, _, state) do
    {{pid, _} = from, ts, request} = Map.get(state.requests, {:request_id, response.request_id})

    duration = System.monotonic_time(:millisecond) - ts

    Logger.debug(
      "Created producer #{request.producer_id} on broker #{state.broker_name} after #{duration}ms"
    )

    requests = Map.delete(state.requests, {:request_id, response.request_id})

    reply = %{
      producer_id: request.producer_id,
      producer_name: response.producer_name,
      last_sequence_id: response.last_sequence_id,
      max_message_size: state.max_message_size,
      producer_access_mode: request.producer_access_mode,
      properties: from_kv(request.metadata)
    }

    GenServer.reply(from, {:ok, reply})

    :telemetry.execute(
      [:pulsar_ex, :connection, :create_producer, :success],
      %{count: 1, duration: duration},
      state.metadata
    )

    ref = Process.monitor(pid)

    producers = Map.put(state.producers, request.producer_id, {pid, ref})
    %{state | requests: requests, producers: producers}
  end

  defp handle_command(%CommandProducerSuccess{} = response, _, state) do
    {_, ts, request} = Map.get(state.requests, {:request_id, response.request_id})

    duration = System.monotonic_time(:millisecond) - ts

    Logger.warn(
      "Producer #{request.producer_id} not ready on broker #{state.broker_name}, after #{duration}ms"
    )

    state
  end

  defp handle_command(%CommandSuccess{} = response, _, state) do
    {request_info, requests} = Map.pop(state.requests, {:request_id, response.request_id})
    state = %{state | requests: requests}

    case request_info do
      {{pid, _} = from, ts, %CommandSubscribe{} = request} ->
        duration = System.monotonic_time(:millisecond) - ts

        Logger.debug(
          "Subscribed consumer #{request.consumer_id} on broker #{state.broker_name}, after #{
            duration
          }ms"
        )

        reply = %{
          consumer_id: request.consumer_id,
          consumer_name: request.consumer_name,
          subscription_type: request.subType,
          priority_level: request.priority_level,
          read_compacted: request.read_compacted,
          initial_position: request.initialPosition,
          properties: from_kv(request.metadata)
        }

        GenServer.reply(from, {:ok, reply})

        :telemetry.execute(
          [:pulsar_ex, :connection, :subscribe, :success],
          %{count: 1, duration: duration},
          state.metadata
        )

        ref = Process.monitor(pid)

        consumers = Map.put(state.consumers, request.consumer_id, {pid, ref})
        %{state | consumers: consumers}

      {nil, ts, %CommandCloseProducer{producer_id: producer_id}} ->
        duration = System.monotonic_time(:millisecond) - ts

        Logger.debug(
          "Stopped producer #{producer_id} from broker #{state.broker_name}, after #{duration}ms"
        )

        state

      {nil, ts, %CommandCloseConsumer{consumer_id: consumer_id}} ->
        duration = System.monotonic_time(:millisecond) - ts

        Logger.debug(
          "Stopped consumer #{consumer_id} from broker #{state.broker_name}, after #{duration}ms"
        )

        state
    end
  end

  defp handle_command(%CommandError{error: err} = response, _, state) do
    {request_info, requests} = Map.pop(state.requests, {:request_id, response.request_id})
    state = %{state | requests: requests}

    case request_info do
      {from, ts, %CommandProducer{} = request} ->
        duration = System.monotonic_time(:millisecond) - ts

        Logger.error(
          "Error connecting producer #{request.producer_id} on broker #{state.broker_name}, after #{
            duration
          }ms, #{inspect(err)}"
        )

        GenServer.reply(from, {:error, err})

        :telemetry.execute(
          [:pulsar_ex, :connection, :create_producer, :error],
          %{count: 1},
          state.metadata
        )

        state

      {from, ts, %CommandSubscribe{} = request} ->
        duration = System.monotonic_time(:millisecond) - ts

        Logger.error(
          "Error subscribing to topic #{request.topic} for consumer #{request.consumer_id} on broker #{
            state.broker_name
          }, after #{duration}ms, #{inspect(err)}"
        )

        GenServer.reply(from, {:error, err})

        :telemetry.execute(
          [:pulsar_ex, :connection, :subscribe, :error],
          %{count: 1},
          state.metadata
        )

        state

      {nil, ts, %CommandCloseProducer{producer_id: producer_id}} ->
        duration = System.monotonic_time(:millisecond) - ts

        Logger.error(
          "Error stopping producer #{producer_id} from broker #{state.broker_name}, after #{
            duration
          }ms, #{inspect(err)}"
        )

        state

      {nil, ts, %CommandCloseConsumer{consumer_id: consumer_id}} ->
        duration = System.monotonic_time(:millisecond) - ts

        Logger.error(
          "Error stopping consumer #{consumer_id} from broker #{state.broker_name}, after #{
            duration
          }ms, #{inspect(err)}"
        )

        state
    end
  end

  defp handle_command(%CommandSendReceipt{} = response, _, state) do
    {{from, ts, _}, requests} =
      Map.pop(state.requests, {:sequence_id, response.producer_id, response.sequence_id})

    state = %{state | requests: requests}

    duration = System.monotonic_time(:millisecond) - ts

    Logger.debug(
      "Received Send Receipt from broker #{state.broker_name} for producer #{response.producer_id}, after #{
        duration
      }ms"
    )

    GenServer.reply(from, {:ok, response.message_id})

    :telemetry.execute(
      [:pulsar_ex, :connection, :send, :success],
      %{count: 1, duration: duration},
      state.metadata
    )

    state
  end

  defp handle_command(%CommandSendError{error: err} = response, _, state) do
    {{from, ts, _}, requests} =
      Map.pop(state.requests, {:sequence_id, response.producer_id, response.sequence_id})

    state = %{state | requests: requests}

    duration = System.monotonic_time(:millisecond) - ts

    Logger.error(
      "Received Send Error from broker #{state.broker_name} for producer #{response.producer_id}, #{
        inspect(err)
      }, after #{duration}ms"
    )

    GenServer.reply(from, {:error, err})

    :telemetry.execute(
      [:pulsar_ex, :connection, :send, :error],
      %{count: 1},
      state.metadata
    )

    state
  end

  defp handle_command(
         %CommandAckResponse{request_id: request_id, error: nil} = response,
         _,
         state
       ) do
    {{nil, ts, request}, requests} = Map.pop(state.requests, {:request_id, request_id})
    state = %{state | requests: requests}

    duration = System.monotonic_time(:millisecond) - ts

    Logger.debug(
      "Received Ack Response from broker #{state.broker_name} for consumer #{request.consumer_id}, #{
        inspect(duration)
      }ms, #{inspect(response)}"
    )

    state
  end

  defp handle_command(%CommandAckResponse{request_id: request_id} = response, _, state) do
    {{nil, ts, request}, requests} = Map.pop(state.requests, {:request_id, request_id})
    state = %{state | requests: requests}

    duration = System.monotonic_time(:millisecond) - ts

    Logger.error(
      "Received Ack Error from broker #{state.broker_name} for consumer #{request.consumer_id}, #{
        inspect(response)
      }, after #{duration}ms"
    )

    state
  end
end
