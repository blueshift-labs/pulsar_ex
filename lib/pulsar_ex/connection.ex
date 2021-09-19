defmodule PulsarEx.Connection do
  defmodule State do
    @enforce_keys [
      :broker,
      :broker_name,
      :request_id,
      :producer_id,
      :consumer_id,
      :requests,
      :producers,
      :consumers,
      :buffer
    ]

    defstruct [
      :socket,
      :max_message_size,
      :last_server_ts,
      :broker,
      :broker_name,
      :request_id,
      :producer_id,
      :consumer_id,
      :requests,
      :producers,
      :consumers,
      :buffer
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
    CommandRedeliverUnacknowledgedMessages,
    CommandAckResponse,
    CommandPartitionedTopicMetadataResponse,
    CommandPartitionedTopicMetadata,
    CommandLookupTopicResponse,
    CommandLookupTopic,
    CommandMessage
  }

  @client_version "Pulsar Ex #{Mix.Project.config()[:version]}"
  @protocol_version 13

  @timeout 5000
  @ping_interval 45_000

  def create_producer(conn, topic, opts \\ []) do
    GenServer.call(conn, {:create_producer, topic, opts})
  end

  def send_message(conn, %ProducerMessage{} = message) do
    GenServer.call(conn, {:send, message})
  end

  def send_messages(conn, messages) when is_list(messages) do
    GenServer.call(conn, {:send, messages})
  end

  def subscribe(conn, topic, subscription, sub_type, opts \\ []) do
    GenServer.call(conn, {:subscribe, topic, subscription, sub_type, opts})
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

  def lookup_partitions(conn, topic) do
    GenServer.call(conn, {:lookup_partitions, topic})
  end

  def lookup_topic(conn, topic) do
    GenServer.call(conn, {:lookup_topic, topic})
  end

  def start_link(%Broker{} = broker) do
    Connection.start_link(__MODULE__, broker)
  end

  @impl true
  def init(broker) do
    Logger.debug("Starting connection with broker #{Broker.to_name(broker)}")

    Process.flag(:trap_exit, true)

    state = %State{
      broker: broker,
      broker_name: Broker.to_name(broker),
      request_id: 0,
      producer_id: 0,
      consumer_id: 0,
      requests: %{},
      producers: %{},
      consumers: %{},
      buffer: <<>>
    }

    {:connect, :init, state}
  end

  @impl true
  def connect(:init, %{broker: broker} = state) do
    with {:ok, socket} <- do_connect(broker.host, broker.port),
         {:ok, max_message_size} <- do_handshake(socket) do
      :inet.setopts(socket, active: :once)
      Process.send_after(self(), :send_ping, @ping_interval)
      Logger.debug("Connection established with broker #{state.broker_name}")

      {:ok,
       %{state | socket: socket, last_server_ts: Timex.now(), max_message_size: max_message_size}}
    else
      {:error, _} = err ->
        {:stop, err, state}
    end
  end

  @impl true
  def disconnect(err, state) do
    Logger.error("Disconnecting from broker #{state.broker_name}, for error #{inspect(err)}")

    :gen_tcp.close(state.socket)

    {:stop, err, state}
  end

  @impl true
  def terminate(reason, state) do
    case reason do
      :normal ->
        Logger.debug("Stopping connection with broker #{state.broker_name}}, #{inspect(reason)}")

      :shutdown ->
        Logger.debug("Stopping connection with broker #{state.broker_name}}, #{inspect(reason)}")

      {:shutdown, reason} ->
        Logger.debug("Stopping connection with broker #{state.broker_name}}, #{inspect(reason)}")

      _ ->
        Logger.error("Stopping connection with broker #{state.broker_name}}, #{inspect(reason)}")
    end

    state
  end

  defp do_connect(host, port) do
    socket_opts = Application.get_env(:pulsar_ex, :socket_opts, []) |> optimize_socket_opts()
    timeout = Application.get_env(:pulsar_ex, :timeout, @timeout)
    :gen_tcp.connect(to_charlist(host), port, socket_opts, timeout)
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
    Logger.debug("Creating producer for topic #{topic}, on broker #{state.broker_name}")

    request =
      CommandProducer.new(
        topic: topic,
        request_id: state.request_id,
        producer_id: state.producer_id
      )

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

    state = %{state | request_id: state.request_id + 1, producer_id: state.producer_id + 1}

    case :gen_tcp.send(state.socket, encode_command(request)) do
      :ok ->
        requests =
          Map.put(state.requests, {:request_id, request.request_id}, {from, Timex.now(), request})

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call({:subscribe, topic, subscription, sub_type, opts}, from, state) do
    Logger.debug(
      "Subscribing to topic #{topic} with subscription #{subscription}, on broker #{
        state.broker_name
      }"
    )

    request =
      CommandSubscribe.new(
        topic: topic,
        subscription: subscription,
        subType: subscription_type(sub_type),
        consumer_id: state.consumer_id,
        request_id: state.request_id
      )

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

    state = %{state | consumer_id: request.consumer_id + 1, request_id: request.request_id + 1}

    case :gen_tcp.send(state.socket, encode_command(request)) do
      :ok ->
        requests =
          Map.put(state.requests, {:request_id, request.request_id}, {from, Timex.now(), request})

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call({:flow_permits, consumer_id, permits}, _, state) do
    Logger.debug(
      "Sending flow permits #{permits} to broker #{state.broker_name} for consumer #{consumer_id}"
    )

    command =
      CommandFlow.new(
        consumer_id: consumer_id,
        messagePermits: permits
      )

    case :gen_tcp.send(state.socket, encode_command(command)) do
      :ok ->
        {:reply, :ok, state}

      {:error, _} = err ->
        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call({:ack, consumer_id, ack_type, msg_ids}, from, state) when is_list(msg_ids) do
    Logger.debug(
      "Sending #{length(msg_ids)} acks to broker #{state.broker_name} for consumer #{consumer_id}"
    )

    request =
      CommandAck.new(
        ack_type: ack_type(ack_type),
        consumer_id: consumer_id,
        request_id: state.request_id,
        message_id: msg_ids,
        txnid_least_bits: nil,
        txnid_most_bits: nil
      )

    state = %{state | request_id: request.request_id + 1}

    case :gen_tcp.send(state.socket, encode_command(request)) do
      :ok ->
        requests =
          Map.put(state.requests, {:request_id, request.request_id}, {from, Timex.now(), request})

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call({:redeliver, consumer_id, msg_ids}, _, state) when is_list(msg_ids) do
    Logger.debug(
      "Sending #{length(msg_ids)} redeliver to broker #{state.broker_name} for consumer #{
        consumer_id
      }"
    )

    command =
      CommandRedeliverUnacknowledgedMessages.new(
        consumer_id: consumer_id,
        message_ids: msg_ids
      )

    case :gen_tcp.send(state.socket, encode_command(command)) do
      :ok ->
        {:reply, :ok, state}

      {:error, _} = err ->
        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call(
        {:send, %ProducerMessage{producer_id: producer_id, sequence_id: sequence_id} = message},
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
            {from, Timex.now(), request}
          )

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call(
        {:send,
         [%ProducerMessage{producer_id: producer_id, sequence_id: sequence_id} | _] = messages},
        from,
        state
      ) do
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
            {from, Timex.now(), request}
          )

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call({:lookup_partitions, topic}, from, state) do
    Logger.debug("Looking up topic partitions on broker #{state.broker_name} for topic #{topic}")

    request =
      CommandPartitionedTopicMetadata.new(
        topic: topic,
        request_id: state.request_id
      )

    state = %{state | request_id: request.request_id + 1}

    case :gen_tcp.send(state.socket, encode_command(request)) do
      :ok ->
        requests =
          Map.put(state.requests, {:request_id, request.request_id}, {from, Timex.now(), request})

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
        {:disconnect, err, err, state}
    end
  end

  @impl true
  def handle_call({:lookup_topic, topic}, from, state) do
    Logger.debug("Looking up topic on broker #{state.broker_name} for topic #{topic}")

    request =
      CommandLookupTopic.new(
        topic: topic,
        request_id: state.request_id
      )

    state = %{state | request_id: request.request_id + 1}

    case :gen_tcp.send(state.socket, encode_command(request)) do
      :ok ->
        requests =
          Map.put(state.requests, {:request_id, request.request_id}, {from, Timex.now(), request})

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
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
      msgs = msgs ++ Map.get(acc, command.consumer_id, [])
      Map.put(acc, command.consumer_id, msgs)
    end)
    |> Enum.each(fn {consumer_id, msgs} ->
      case Enum.find(state.consumers, &match?({_, ^consumer_id}, &1)) do
        nil ->
          Logger.warn(
            "Received #{length(msgs)} messages from broker #{state.broker_name} for missing consumer #{
              consumer_id
            }"
          )

        {consumer, _} ->
          Logger.debug(
            "Received #{length(msgs)} messages from broker #{state.broker_name} for consumer #{
              consumer_id
            }"
          )

          GenServer.cast(consumer, {:messages, msgs})
      end
    end)

    :inet.setopts(socket, active: :once)
    {:noreply, %{state | buffer: buffer, last_server_ts: Timex.now()}}
  end

  @impl true
  def handle_info(:send_ping, state) do
    Logger.debug("Sending ping command to broker #{state.broker_name}")

    cond do
      Timex.after?(
        Timex.now(),
        Timex.add(state.last_server_ts, Timex.Duration.from_milliseconds(@ping_interval))
      ) ->
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
    Logger.debug("Sending pong command to broker #{state.broker_name}")

    case :gen_tcp.send(state.socket, encode_command(CommandPong.new())) do
      :ok -> {:noreply, state}
      {:error, _} = err -> {:disconnect, err, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, _, pid, {:shutdown, :close}}, state) do
    Process.demonitor(ref)

    producer_id = Map.get(state.producers, pid)
    consumer_id = Map.get(state.consumers, pid)

    state =
      case {producer_id, consumer_id} do
        {producer_id, nil} ->
          Logger.debug("Detected producer #{producer_id} down on broker #{state.broker_name}")

          %{state | producers: Map.delete(state.producers, pid)}

        {nil, consumer_id} ->
          Logger.debug("Detected consumer #{consumer_id} down on broker #{state.broker_name}")

          %{state | consumers: Map.delete(state.consumers, pid)}
      end

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, _, pid, _}, state) do
    Process.demonitor(ref)

    producer_id = Map.get(state.producers, pid)
    consumer_id = Map.get(state.consumers, pid)

    {request, state} =
      case {producer_id, consumer_id} do
        {producer_id, nil} ->
          Logger.debug("Detected producer #{producer_id} down on broker #{state.broker_name}")

          producers = Map.delete(state.producers, pid)

          request =
            CommandCloseProducer.new(
              request_id: state.request_id,
              producer_id: producer_id
            )

          {request, %{state | producers: producers, request_id: state.request_id + 1}}

        {nil, consumer_id} ->
          Logger.debug("Detected consumer #{consumer_id} down on broker #{state.broker_name}")

          consumers = Map.delete(state.consumers, pid)

          request =
            CommandCloseConsumer.new(
              request_id: state.request_id,
              consumer_id: consumer_id
            )

          {request, %{state | consumers: consumers, request_id: state.request_id + 1}}
      end

    case :gen_tcp.send(state.socket, encode_command(request)) do
      :ok ->
        requests =
          Map.put(state.requests, {:request_id, request.request_id}, {nil, Timex.now(), request})

        {:noreply, %{state | requests: requests}}

      {:error, _} = err ->
        {:disconnect, err, state}
    end
  end

  # ================== handle_command! =====================
  defp handle_command(%CommandPing{}, _, state) do
    Logger.debug("Received ping command from broker #{state.broker_name}")

    Process.send(self(), :send_pong, [])
    state
  end

  defp handle_command(%CommandPong{}, _, state) do
    Logger.debug("Received pong command from broker #{state.broker_name}")

    state
  end

  defp handle_command(%CommandProducerSuccess{producer_ready: true} = response, _, state) do
    {{pid, _} = from, ts, request} = Map.get(state.requests, {:request_id, response.request_id})

    latency = Timex.diff(Timex.now(), ts, :milliseconds)

    Logger.debug(
      "Created producer #{request.producer_id} on broker #{state.broker_name} after #{latency}ms"
    )

    Process.monitor(pid)

    producers = Map.put(state.producers, pid, request.producer_id)
    requests = Map.delete(state.requests, {:request_id, response.request_id})

    reply = %{
      topic: request.topic,
      producer_id: request.producer_id,
      producer_name: response.producer_name,
      producer_access_mode: request.producer_access_mode,
      last_sequence_id: response.last_sequence_id,
      max_message_size: state.max_message_size,
      properties: from_kv(request.metadata),
      connection: self()
    }

    GenServer.reply(from, {:ok, reply})
    %{state | requests: requests, producers: producers}
  end

  defp handle_command(%CommandProducerSuccess{} = response, _, state) do
    {_, ts, request} = Map.get(state.requests, {:request_id, response.request_id})

    latency = Timex.diff(Timex.now(), ts, :milliseconds)

    Logger.warn(
      "Producer #{request.producer_id} not ready on broker #{state.broker_name}, after #{latency}ms"
    )

    state
  end

  defp handle_command(%CommandSuccess{} = response, _, state) do
    {request_info, requests} = Map.pop(state.requests, {:request_id, response.request_id})
    state = %{state | requests: requests}

    case request_info do
      {{pid, _} = from, ts, %CommandSubscribe{} = request} ->
        latency = Timex.diff(Timex.now(), ts, :milliseconds)

        Logger.debug(
          "Subscribed to topic #{request.topic} for consumer #{request.consumer_id} on broker #{
            state.broker_name
          }, after #{latency}ms"
        )

        Process.monitor(pid)

        consumers = Map.put(state.consumers, pid, request.consumer_id)

        reply = %{
          topic: request.topic,
          subscription: request.subscription,
          subscription_type: request.subType,
          priority_level: request.priority_level,
          read_compacted: request.read_compacted,
          initial_position: request.initialPosition,
          consumer_id: request.consumer_id,
          consumer_name: request.consumer_name,
          properties: request.metadata,
          connection: self()
        }

        GenServer.reply(from, {:ok, reply})

        %{state | consumers: consumers}

      {nil, ts, %CommandCloseProducer{producer_id: producer_id}} ->
        latency = Timex.diff(Timex.now(), ts, :milliseconds)

        Logger.debug(
          "Stopped producer #{producer_id} from broker #{state.broker_name}, after #{latency}ms"
        )

        state

      {nil, ts, %CommandCloseConsumer{consumer_id: consumer_id}} ->
        latency = Timex.diff(Timex.now(), ts, :milliseconds)

        Logger.debug(
          "Stopped consumer #{consumer_id} from broker #{state.broker_name}, after #{latency}ms"
        )

        state

      {from, ts, request} ->
        latency = Timex.diff(Timex.now(), ts, :milliseconds)

        Logger.warn(
          "Received unhandled success command from broker #{state.broker_name} for request #{
            inspect(request)
          }, after #{latency}ms"
        )

        GenServer.reply(from, :ok)
        state
    end
  end

  defp handle_command(%CommandError{error: err} = response, _, state) do
    {request_info, requests} = Map.pop(state.requests, {:request_id, response.request_id})
    state = %{state | requests: requests}

    case request_info do
      {nil, ts, request} ->
        latency = Timex.diff(Timex.now(), ts, :milliseconds)

        Logger.error(
          "Received error #{inspect(err)} from broker #{state.broker_name} for request #{
            inspect(request)
          }, after #{latency}ms"
        )

        state

      {from, ts, request} ->
        latency = Timex.diff(Timex.now(), ts, :milliseconds)

        Logger.error(
          "Received error #{inspect(err)} from broker #{state.broker_name} for request #{
            inspect(request)
          }, after #{latency}ms"
        )

        GenServer.reply(from, {:error, err})
        state
    end
  end

  defp handle_command(%CommandCloseProducer{producer_id: producer_id}, _, state) do
    case Enum.find(state.producers, &match?({_, ^producer_id}, &1)) do
      {producer, producer_id} ->
        Logger.warn(
          "Received Close Producer command from broker #{state.broker_name} for producer #{
            producer_id
          }"
        )

        GenServer.cast(producer, :close)

        state

      nil ->
        Logger.error(
          "Received Close Producer command from broker #{state.broker_name} for missing producer"
        )

        state
    end
  end

  defp handle_command(%CommandCloseConsumer{consumer_id: consumer_id}, _, state) do
    case Enum.find(state.consumers, &match?({_, ^consumer_id}, &1)) do
      {consumer, consumer_id} ->
        Logger.warn(
          "Received Close Consumer command from broker #{state.broker_name} for consumer #{
            consumer_id
          }"
        )

        GenServer.cast(consumer, :close)
        state

      nil ->
        Logger.error(
          "Received Close Consumer command from broker #{state.broker_name} for missing consumer"
        )

        state
    end
  end

  defp handle_command(%CommandSendReceipt{} = response, _, state) do
    {{from, ts, _}, requests} =
      Map.pop(state.requests, {:sequence_id, response.producer_id, response.sequence_id})

    state = %{state | requests: requests}

    latency = Timex.diff(Timex.now(), ts, :milliseconds)

    Logger.debug(
      "Received Send Receipt from broker #{state.broker_name} for producer #{response.producer_id}, after #{
        latency
      }ms"
    )

    if from != nil do
      GenServer.reply(from, {:ok, response.message_id})
    end

    state
  end

  defp handle_command(%CommandSendError{error: err} = response, _, state) do
    {{from, ts, _}, requests} =
      Map.pop(state.requests, {:sequence_id, response.producer_id, response.sequence_id})

    state = %{state | requests: requests}

    latency = Timex.diff(Timex.now(), ts, :milliseconds)

    Logger.error(
      "Received Send Error from broker #{state.broker_name} for producer #{response.producer_id}, #{
        inspect(err)
      }, after #{latency}ms"
    )

    if from != nil do
      GenServer.reply(from, {:error, err})
    end

    state
  end

  defp handle_command(%CommandAckResponse{request_id: request_id, error: nil}, _, state) do
    {{from, ts, request}, requests} = Map.pop(state.requests, {:request_id, request_id})
    state = %{state | requests: requests}

    latency = Timex.diff(Timex.now(), ts, :milliseconds)

    Logger.debug(
      "Received Ack Response from broker #{state.broker_name} for consumer #{request.consumer_id}, #{
        inspect(latency)
      }ms"
    )

    if from != nil do
      GenServer.reply(from, :ok)
    end

    state
  end

  defp handle_command(%CommandAckResponse{request_id: request_id, error: err}, _, state) do
    {{from, ts, request}, requests} = Map.pop(state.requests, {:request_id, request_id})
    state = %{state | requests: requests}

    latency = Timex.diff(Timex.now(), ts, :milliseconds)

    Logger.error(
      "Received Ack Error from broker #{state.broker_name} for consumer #{request.consumer_id}, #{
        inspect(err)
      }, after #{latency}ms"
    )

    if from != nil do
      GenServer.reply(from, {:error, err})
    end

    state
  end

  defp handle_command(
         %CommandPartitionedTopicMetadataResponse{request_id: request_id, error: nil} = response,
         _,
         state
       ) do
    {{from, ts, request}, requests} = Map.pop(state.requests, {:request_id, request_id})
    state = %{state | requests: requests}

    latency = Timex.diff(Timex.now(), ts, :milliseconds)

    Logger.debug(
      "Received Partition Metadata from broker #{state.broker_name} for topic #{request.topic}, after #{
        latency
      }ms"
    )

    if from != nil do
      GenServer.reply(from, {:ok, response.partitions})
    end

    state
  end

  defp handle_command(
         %CommandPartitionedTopicMetadataResponse{request_id: request_id, error: err},
         _,
         state
       ) do
    {{from, ts, request}, requests} = Map.pop(state.requests, {:request_id, request_id})
    state = %{state | requests: requests}

    latency = Timex.diff(Timex.now(), ts, :milliseconds)

    Logger.error(
      "Received Partition Metadata Error from broker #{state.broker_name} for topic #{
        request.topic
      }, #{inspect(err)}, #{latency}ms"
    )

    if from != nil do
      GenServer.reply(from, {:error, err})
    end

    state
  end

  defp handle_command(
         %CommandLookupTopicResponse{request_id: request_id, error: nil} = response,
         _,
         state
       ) do
    {{from, ts, request}, requests} = Map.pop(state.requests, {:request_id, request_id})
    state = %{state | requests: requests}

    latency = Timex.diff(Timex.now(), ts, :milliseconds)

    Logger.debug(
      "Received Topic Metadata from broker #{state.broker_name} for topic #{request.topic}, after #{
        latency
      }ms"
    )

    if from != nil do
      GenServer.reply(from, {:ok, response})
    end

    state
  end

  defp handle_command(%CommandLookupTopicResponse{request_id: request_id, error: err}, _, state) do
    {{from, ts, request}, requests} = Map.pop(state.requests, {:request_id, request_id})
    state = %{state | requests: requests}

    latency = Timex.diff(Timex.now(), ts, :milliseconds)

    Logger.error(
      "Received Topic Metadata Error from broker #{state.broker_name} for topic #{request.topic}, #{
        inspect(err)
      }, after #{latency}ms"
    )

    if from != nil do
      GenServer.reply(from, {:error, err})
    end

    state
  end

  # we don't want to handle consumer message here, we will bundle them to consumers
  defp handle_command(%CommandMessage{}, _, state), do: state

  defp handle_command(command, _, state) do
    Logger.warn("Received unhandled command #{inspect(command)} from broker #{state.broker_name}")

    state
  end
end
