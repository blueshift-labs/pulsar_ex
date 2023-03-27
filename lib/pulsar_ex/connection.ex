defmodule PulsarEx.Connection do
  @enforce_keys [
    :cluster,
    :broker,
    :attempts,
    :backoff,
    :last_request_id,
    :requests,
    :buffer,
    :socket
  ]

  defstruct [
    :cluster,
    :broker,
    :attempts,
    :backoff,
    :last_request_id,
    :requests,
    :buffer,
    :socket,
    :max_message_size,
    :error
  ]

  use Connection

  import PulsarEx.IO

  require Logger

  alias PulsarEx.{
    Cluster,
    Broker,
    Backoff,
    ProducerMessage,
    Producer,
    Consumer,
    ConsumerIDRegistry,
    ProducerIDRegistry
  }

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
    CommandLookupTopic,
    CommandLookupTopicResponse,
    CommandPartitionedTopicMetadata,
    CommandPartitionedTopicMetadataResponse,
    MessageIdData
  }

  defmodule LookupCallback do
    @callback lookup_topic_partitions(
                conn :: pid(),
                topic_name :: String.t(),
                deadline :: integer()
              ) :: {:ok, integer()} | {:error, term()}
    @callback lookup_topic(
                conn :: pid(),
                topic_name :: String.t(),
                authoritative :: boolean(),
                deadline :: integer()
              ) ::
                {:connect, String.t()} | {:redirect, String.t()} | {:error, term()}
  end

  defmodule ProducerCallback do
    @callback create_producer(
                conn :: pid(),
                producer_id :: integer(),
                topic_name :: String.t(),
                producer_opts :: keyword(),
                deadline :: integer()
              ) :: {:ok, map()} | {:error, term()}
    @callback send_message(
                conn :: pid(),
                producer_id :: integer(),
                sequence_id :: integer(),
                message :: %ProducerMessage{},
                deadline :: integer()
              ) :: :ok | {:error, term()}
    @callback send_messages(
                conn :: pid(),
                producer_id :: integer(),
                sequence_id :: integer(),
                messages :: list(%ProducerMessage{}),
                deadline :: integer()
              ) :: :ok | {:error, term()}
    @callback close_producer(conn :: pid(), producer_id :: integer()) :: :ok | {:error, term()}
  end

  defmodule ConsumerCallback do
    @type message_id() :: {integer(), integer()}

    @callback subscribe(
                conn :: pid(),
                consumer_id :: integer(),
                topic_name :: String.t(),
                subscription :: String.t() | atom(),
                sub_type :: String.t() | atom(),
                consumer_opts :: keyword(),
                deadline :: integer()
              ) :: {:ok, map()} | {:error, term()}
    @callback ack(
                conn :: pid(),
                consumer_id :: integer(),
                ack_type :: atom(),
                msg_ids :: list(message_id()),
                deadline :: integer()
              ) :: :ok | {:error, term()}
    @callback flow_permits(conn :: pid(), consumer_id :: integer(), permits :: integer()) ::
                :ok | {:error, term()}
    @callback redeliver(conn :: pid(), consumer_id :: integer(), msg_ids :: list(message_id())) ::
                :ok | {:error, term()}
    @callback close_consumer(conn :: pid(), consumer_id :: integer()) :: :ok | {:error, term()}
  end

  @behaviour LookupCallback
  @behaviour ProducerCallback
  @behaviour ConsumerCallback

  @client_version "PulsarEx #{Mix.Project.config()[:version]}"
  @protocol_version 13

  @connection_timeout 5_000
  @ping_interval 30_000

  @max_attempts 5
  @backoff_type :rand_exp
  @backoff_min 1_000
  @backoff_max 10_000
  @backoff Backoff.new(
             backoff_type: @backoff_type,
             backoff_min: @backoff_min,
             backoff_max: @backoff_max
           )

  @request_timeout 30_000
  @tick_interval 1000

  def start_link({%Cluster{} = cluster, %Broker{} = broker}) do
    Connection.start_link(__MODULE__, {cluster, broker})
  end

  @impl LookupCallback
  def lookup_topic_partitions(conn, topic_name, deadline) do
    timeout = 2 * (deadline - System.monotonic_time(:millisecond))
    timeout = max(timeout, @request_timeout)
    GenServer.call(conn, {:lookup_topic_partitions, topic_name, deadline}, timeout)
  end

  @impl LookupCallback
  def lookup_topic(conn, topic_name, authoritative, deadline) do
    timeout = 2 * (deadline - System.monotonic_time(:millisecond))
    timeout = max(timeout, @request_timeout)
    GenServer.call(conn, {:lookup_topic, topic_name, authoritative, deadline}, timeout)
  end

  @impl ProducerCallback
  def create_producer(conn, producer_id, topic_name, producer_opts, deadline) do
    timeout = 2 * (deadline - System.monotonic_time(:millisecond))
    timeout = max(timeout, @request_timeout)

    GenServer.call(
      conn,
      {:create_producer, producer_id, topic_name, producer_opts, deadline},
      timeout
    )
  end

  @impl ProducerCallback
  def send_message(conn, producer_id, sequence_id, %ProducerMessage{} = message, deadline) do
    timeout = 2 * (deadline - System.monotonic_time(:millisecond))
    timeout = max(timeout, @request_timeout)
    GenServer.call(conn, {:send, producer_id, sequence_id, message, deadline}, timeout)
  end

  @impl ProducerCallback
  def send_messages(conn, producer_id, sequence_id, [%ProducerMessage{} | _] = messages, deadline)
      when is_list(messages) do
    timeout = 2 * (deadline - System.monotonic_time(:millisecond))
    timeout = max(timeout, @request_timeout)
    GenServer.call(conn, {:send, producer_id, sequence_id, messages, deadline}, timeout)
  end

  @impl ProducerCallback
  def close_producer(conn, producer_id) do
    GenServer.cast(conn, {:close_producer, producer_id})
  end

  @impl ConsumerCallback
  def subscribe(conn, consumer_id, topic_name, subscription, sub_type, consumer_opts, deadline) do
    timeout = 2 * (deadline - System.monotonic_time(:millisecond))
    timeout = max(timeout, @request_timeout)

    GenServer.call(
      conn,
      {:subscribe, consumer_id, topic_name, subscription, sub_type, consumer_opts, deadline},
      timeout
    )
  end

  @impl ConsumerCallback
  def ack(conn, consumer_id, ack_type, msg_ids, deadline) do
    timeout = 2 * (deadline - System.monotonic_time(:millisecond))
    timeout = max(timeout, @request_timeout)
    GenServer.call(conn, {:ack, consumer_id, ack_type, msg_ids, deadline}, timeout)
  end

  @impl ConsumerCallback
  def flow_permits(conn, consumer_id, permits) do
    GenServer.call(conn, {:flow_permits, consumer_id, permits})
  end

  @impl ConsumerCallback
  def redeliver(conn, consumer_id, msg_ids) do
    GenServer.call(conn, {:redeliver, consumer_id, msg_ids})
  end

  @impl ConsumerCallback
  def close_consumer(conn, consumer_id) do
    GenServer.cast(conn, {:close_consumer, consumer_id})
  end

  @impl true
  def init({%Cluster{} = cluster, broker}) do
    Logger.metadata(cluster: "#{cluster}", broker: "#{broker}")

    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      cluster: cluster,
      broker: broker,
      attempts: 0,
      backoff: @backoff,
      last_request_id: 0,
      requests: %{},
      buffer: <<>>,
      socket: nil
    }

    Logger.debug("Starting connection on broker")

    Process.send_after(self(), :tick, @tick_interval)

    {:connect, nil, state}
  end

  # ================== connect =====================
  @impl true
  def connect(_, %{attempts: @max_attempts, error: err} = state) do
    Logger.error("Exhausted connection attempts on broker, #{inspect(err)}")

    {:stop, {:shutdown, err}, state}
  end

  def connect(
        _,
        %{
          cluster: %Cluster{cluster_opts: cluster_opts},
          broker: %Broker{host: host, port: port},
          attempts: attempts,
          backoff: backoff
        } = state
      ) do
    with {:ok, socket} <- do_connect(host, port, cluster_opts),
         {:ok, max_message_size} <- do_handshake(socket) do
      :inet.setopts(socket, active: true)
      Process.send_after(self(), :send_ping, @ping_interval)

      Logger.debug("Started connection on broker")

      :telemetry.execute(
        [:pulsar_ex, :connection, :success],
        %{count: 1},
        state
      )

      {:ok,
       %{
         state
         | attempts: 0,
           backoff: @backoff,
           error: nil,
           socket: socket,
           max_message_size: max_message_size
       }}
    else
      {:error, err} ->
        Logger.error("Error starting connection on broker, #{inspect(err)}")

        :telemetry.execute(
          [:pulsar_ex, :connection, :error],
          %{count: 1},
          state
        )

        {wait, backoff} = Backoff.backoff(backoff)
        {:backoff, wait, %{state | attempts: attempts + 1, backoff: backoff, error: err}}
    end
  end

  # ================== handle_cast =====================
  @impl true
  def handle_cast({:close_producer, producer_id}, %{socket: socket, requests: requests} = state) do
    Logger.debug("Sending CommandCloseProducer for producer [#{producer_id}] on broker")

    {request_id, state} = get_request_id(state)

    request =
      CommandCloseProducer.new(
        request_id: request_id,
        producer_id: producer_id
      )

    case :gen_tcp.send(socket, encode_command(request)) do
      :ok ->
        Logger.debug("Sent CommandCloseProducer for producer [#{producer_id}] on broker")

        send_ts = System.monotonic_time()
        deadline = System.monotonic_time(:millisecond) + @request_timeout

        requests =
          Map.put(
            requests,
            {:request_id, request_id},
            {nil, {send_ts, deadline}, request}
          )

        :telemetry.execute(
          [:pulsar_ex, :connection, :close_producer, :request, :success],
          %{count: 1},
          state
        )

        {:noreply, %{state | requests: requests}}

      {:error, err} ->
        Logger.error(
          "Error sending CommandCloseProducer for producer [#{producer_id}] on broker, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :close_producer, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, state}
    end
  end

  def handle_cast({:close_consumer, consumer_id}, %{socket: socket, requests: requests} = state) do
    Logger.debug("Sending CommandCloseConsumer for consumer [#{consumer_id}] on broker")

    {request_id, state} = get_request_id(state)

    request =
      CommandCloseConsumer.new(
        request_id: request_id,
        consumer_id: consumer_id
      )

    case :gen_tcp.send(socket, encode_command(request)) do
      :ok ->
        Logger.debug("Sent CommandCloseConsumer for consumer [#{consumer_id}] on broker")

        send_ts = System.monotonic_time()
        deadline = System.monotonic_time(:millisecond) + @request_timeout

        requests =
          Map.put(
            requests,
            {:request_id, request_id},
            {nil, {send_ts, deadline}, request}
          )

        :telemetry.execute(
          [:pulsar_ex, :connection, :close_consumer, :request, :success],
          %{count: 1},
          state
        )

        {:noreply, %{state | requests: requests}}

      {:error, err} ->
        Logger.error(
          "Error sending CommandCloseConsumer for consumer [#{consumer_id}] on broker, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :close_consumer, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, state}
    end
  end

  # ================== handle_call =====================
  @impl true
  def handle_call(_, _from, %{socket: nil} = state) do
    {:reply, {:error, :connection_not_ready}, state}
  end

  def handle_call(
        {:create_producer, producer_id, topic_name, producer_opts, deadline},
        from,
        %{socket: socket, requests: requests} = state
      ) do
    Logger.debug(
      "Sending CommandProducer for producer [#{producer_id}] writing to topic [#{topic_name}] on broker"
    )

    {request_id, state} = get_request_id(state)

    request =
      CommandProducer.new(
        request_id: request_id,
        producer_id: producer_id,
        topic: topic_name
      )

    request =
      case Keyword.get(producer_opts, :producer_name) do
        nil ->
          request

        producer_name ->
          %{request | producer_name: producer_name, user_provided_producer_name: true}
      end

    request =
      case Keyword.get(producer_opts, :producer_access_mode) do
        nil ->
          request

        mode ->
          %{request | producer_access_mode: producer_access_mode(mode)}
      end

    request = %{request | metadata: Keyword.get(producer_opts, :properties) |> to_kv()}

    case :gen_tcp.send(socket, encode_command(request)) do
      :ok ->
        Logger.debug(
          "Sent CommandProducer for producer [#{producer_id}] writing to topic [#{topic_name}] on broker"
        )

        send_ts = System.monotonic_time()

        requests =
          Map.put(
            requests,
            {:request_id, request_id},
            {from, {send_ts, deadline}, request}
          )

        :telemetry.execute(
          [:pulsar_ex, :connection, :create_producer, :request, :success],
          %{count: 1},
          state
        )

        {:noreply, %{state | requests: requests}}

      {:error, err} ->
        Logger.error(
          "Error sending CommandProducer for producer [#{producer_id}] writing to topic [#{topic_name}] on broker, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :create_producer, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, {:error, err}, state}
    end
  end

  def handle_call(
        {:subscribe, consumer_id, topic_name, subscription, sub_type, consumer_opts, deadline},
        from,
        %{socket: socket, requests: requests} = state
      ) do
    Logger.debug(
      "Sending CommandSubscribe for consumer [#{consumer_id}] to topic [#{topic_name}] with subscription [#{subscription}] in [#{sub_type}] mode on broker"
    )

    {request_id, state} = get_request_id(state)

    request =
      CommandSubscribe.new(
        request_id: request_id,
        consumer_id: consumer_id,
        topic: topic_name,
        subscription: subscription,
        subType: subscription_type(sub_type)
      )

    request =
      case Keyword.get(consumer_opts, :consumer_name) do
        nil -> request
        consumer_name -> %{request | consumer_name: consumer_name}
      end

    request =
      case Keyword.get(consumer_opts, :priority_level) do
        nil -> request
        priority_level -> %{request | priority_level: priority_level}
      end

    request =
      case Keyword.get(consumer_opts, :durable) do
        nil -> request
        durable -> %{request | durable: durable}
      end

    request =
      case Keyword.get(consumer_opts, :read_compacted) do
        nil -> request
        read_compacted -> %{request | read_compacted: read_compacted}
      end

    request =
      case Keyword.get(consumer_opts, :force_topic_creation) do
        nil -> request
        force_topic_creation -> %{request | force_topic_creation: force_topic_creation}
      end

    request =
      case Keyword.get(consumer_opts, :initial_position) do
        nil -> request
        init_position -> %{request | initialPosition: initial_position(init_position)}
      end

    request = %{request | metadata: Keyword.get(consumer_opts, :properties) |> to_kv()}

    case :gen_tcp.send(socket, encode_command(request)) do
      :ok ->
        Logger.debug(
          "Sent CommandSubscribe for consumer [#{consumer_id}] to topic [#{topic_name}] with subscription [#{subscription}] in [#{sub_type}] mode on broker"
        )

        send_ts = System.monotonic_time()

        requests =
          Map.put(
            requests,
            {:request_id, request_id},
            {from, {send_ts, deadline}, request}
          )

        :telemetry.execute(
          [:pulsar_ex, :connection, :subscribe, :request, :success],
          %{count: 1},
          state
        )

        {:noreply, %{state | requests: requests}}

      {:error, err} ->
        Logger.error(
          "Error sending CommandSubscribe for consumer [#{consumer_id}] to topic [#{topic_name}] with subscription [#{subscription}] in [#{sub_type}] mode on broker, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :subscribe, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, {:error, err}, state}
    end
  end

  def handle_call(
        {:lookup_topic_partitions, topic_name, deadline},
        from,
        %{socket: socket, requests: requests} = state
      ) do
    Logger.debug("Sending CommandPartitionedTopicMetadata for topic [#{topic_name}] on broker")

    {request_id, state} = get_request_id(state)

    request =
      CommandPartitionedTopicMetadata.new(
        request_id: request_id,
        topic: topic_name
      )

    case :gen_tcp.send(socket, encode_command(request)) do
      :ok ->
        Logger.debug("Sent CommandPartitionedTopicMetadata for topic [#{topic_name}] on broker")

        send_ts = System.monotonic_time()

        requests =
          Map.put(
            requests,
            {:request_id, request_id},
            {from, {send_ts, deadline}, request}
          )

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_partitions, :request, :success],
          %{count: 1},
          state
        )

        {:noreply, %{state | requests: requests}}

      {:error, err} ->
        Logger.error(
          "Error sending CommandPartitionedTopicMetadata for topic [#{topic_name}] on broker, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_partitions, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, {:error, err}, state}
    end
  end

  def handle_call(
        {:lookup_topic, topic_name, authoritative, deadline},
        from,
        %{socket: socket, requests: requests} = state
      ) do
    Logger.debug("Sending CommandLookupTopic for topic [#{topic_name}] on broker")

    {request_id, state} = get_request_id(state)

    request =
      CommandLookupTopic.new(
        request_id: request_id,
        topic: topic_name,
        authoritative: authoritative
      )

    case :gen_tcp.send(socket, encode_command(request)) do
      :ok ->
        Logger.debug("Sent CommandLookupTopic for topic [#{topic_name}] on broker")

        send_ts = System.monotonic_time()

        requests =
          Map.put(
            requests,
            {:request_id, request_id},
            {from, {send_ts, deadline}, request}
          )

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic, :request, :success],
          %{count: 1},
          state
        )

        {:noreply, %{state | requests: requests}}

      {:error, err} ->
        Logger.error(
          "Error sending CommandLookupTopic for topic [#{topic_name}] on broker, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, {:error, err}, state}
    end
  end

  def handle_call({:flow_permits, consumer_id, permits}, _from, %{socket: socket} = state) do
    Logger.debug(
      "Sending CommandFlow for consumer [#{consumer_id}] with [#{permits}] permits on broker"
    )

    command =
      CommandFlow.new(
        consumer_id: consumer_id,
        messagePermits: permits
      )

    case :gen_tcp.send(socket, encode_command(command)) do
      :ok ->
        Logger.debug(
          "Sent CommandFlow for consumer [#{consumer_id}] with [#{permits}] permits on broker"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :flow_permits, :request, :success],
          %{count: 1},
          state
        )

        {:reply, :ok, state}

      {:error, err} ->
        Logger.error(
          "Error Sending CommandFlow for consumer [#{consumer_id}] with [#{permits}] permits on broker, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :flow_permits, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, {:error, err}, state}
    end
  end

  def handle_call({:redeliver, consumer_id, msg_ids}, _from, %{socket: socket} = state)
      when is_list(msg_ids) do
    Logger.debug(
      "Sending CommandRedeliverUnacknowledgedMessages for consumer [#{consumer_id}] with #{length(msg_ids)} redeliver on broker"
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

    case :gen_tcp.send(socket, encode_command(command)) do
      :ok ->
        Logger.debug(
          "Sent CommandRedeliverUnacknowledgedMessages for consumer [#{consumer_id}] with #{length(msg_ids)} redeliver on broker"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :redeliver, :request, :success],
          %{count: 1},
          state
        )

        {:reply, :ok, state}

      {:error, err} ->
        Logger.error(
          "Sent CommandRedeliverUnacknowledgedMessages for consumer [#{consumer_id}] with #{length(msg_ids)} redeliver on broker, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :redeliver, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, {:error, err}, state}
    end
  end

  def handle_call(
        {:ack, consumer_id, ack_type, msg_ids, deadline},
        from,
        %{socket: socket, requests: requests} = state
      )
      when is_list(msg_ids) do
    Logger.debug(
      "Sending CommandAck for consumer [#{consumer_id}] with #{length(msg_ids)} acks on broker"
    )

    message_ids =
      Enum.map(msg_ids, fn {ledgerId, entryId} ->
        MessageIdData.new(ledgerId: ledgerId, entryId: entryId)
      end)

    {request_id, state} = get_request_id(state)

    request =
      CommandAck.new(
        request_id: request_id,
        consumer_id: consumer_id,
        ack_type: ack_type(ack_type),
        message_id: message_ids,
        txnid_least_bits: nil,
        txnid_most_bits: nil
      )

    case :gen_tcp.send(socket, encode_command(request)) do
      :ok ->
        Logger.debug(
          "Sent CommandAck for consumer [#{consumer_id}] with #{length(msg_ids)} acks on broker"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :ack, :request, :success],
          %{count: 1},
          state
        )

        send_ts = System.monotonic_time()

        requests =
          Map.put(
            requests,
            {:request_id, request_id},
            {from, {send_ts, deadline}, request}
          )

        {:reply, :ok, %{state | requests: requests}}

      {:error, err} ->
        Logger.error(
          "Error sending CommandAck for consumer [#{consumer_id}] with #{length(msg_ids)} acks on broker, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :ack, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, {:error, err}, state}
    end
  end

  def handle_call(
        {:send, producer_id, sequence_id, messages, deadline},
        from,
        %{socket: socket, requests: requests} = state
      )
      when is_list(messages) do
    Logger.debug(
      "Sending #{length(messages)} messages in batch for producer [#{producer_id}] on broker"
    )

    request = encode_messages(messages)

    case :gen_tcp.send(socket, request) do
      :ok ->
        Logger.debug(
          "Sent #{length(messages)} messages in batch for producer [#{producer_id}] on broker"
        )

        send_ts = System.monotonic_time()

        requests =
          Map.put(
            requests,
            {:sequence_id, producer_id, sequence_id},
            {from, {send_ts, deadline}, request}
          )

        :telemetry.execute(
          [:pulsar_ex, :connection, :send, :request, :success],
          %{count: 1},
          state
        )

        {:reply, :ok, %{state | requests: requests}}

      {:error, err} ->
        Logger.error(
          "Error sending #{length(messages)} messages in batch for producer [#{producer_id}] on broker, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :send, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, {:error, err}, state}
    end
  end

  def handle_call(
        {:send, producer_id, sequence_id, message, deadline},
        from,
        %{socket: socket, requests: requests} = state
      ) do
    Logger.debug("Sending message for producer [#{producer_id}] on broker")

    request = encode_message(message)

    case :gen_tcp.send(socket, request) do
      :ok ->
        Logger.debug("Sent message for producer [#{producer_id}] on broker")

        send_ts = System.monotonic_time()

        requests =
          Map.put(
            requests,
            {:sequence_id, producer_id, sequence_id},
            {from, {send_ts, deadline}, request}
          )

        :telemetry.execute(
          [:pulsar_ex, :connection, :send, :request, :success],
          %{count: 1},
          state
        )

        {:reply, :ok, %{state | requests: requests}}

      {:error, err} ->
        Logger.error(
          "Error sending message for producer [#{producer_id}] on broker, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :send, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, {:error, err}, state}
    end
  end

  # ================== handle_info =====================
  @impl true
  def handle_info({:tcp_passive, _}, state), do: {:noreply, state}

  def handle_info({:tcp_closed, _}, state), do: {:stop, {:shutdown, :closed}, state}

  def handle_info({:tcp_error, _, reason}, state), do: {:stop, {:shutdown, reason}, state}

  def handle_info(
        {:tcp, _socket, data},
        %{cluster: %Cluster{cluster_name: cluster_name}, buffer: buffer} = state
      ) do
    Logger.debug("Receiving data from broker")

    {messages, buffer} = decode(<<buffer::binary, data::binary>>)

    # handle tcp messages other than consumer messages
    state =
      messages
      |> Enum.reduce(state, fn
        {command, payload}, acc -> handle_command(command, payload, acc)
      end)

    # now bundle the consumer messages to consumers
    messages
    |> Enum.filter(&match?({%CommandMessage{}, _}, &1))
    |> Enum.reduce(%{}, fn {%{consumer_id: consumer_id}, msgs}, acc ->
      Map.merge(acc, %{consumer_id => msgs}, fn _, m1, m2 -> m1 ++ m2 end)
    end)
    |> Enum.each(fn {consumer_id, msgs} ->
      Logger.debug("Received #{length(msgs)} messages for consumer [#{consumer_id}] from broker")

      with [{pid, _}] <- Registry.lookup(ConsumerIDRegistry, {cluster_name, consumer_id}) do
        Consumer.messages(pid, msgs)
      else
        _ ->
          Logger.error(
            "Received #{length(msgs)} unexpected messages for consumer [#{consumer_id}] from broker"
          )
      end
    end)

    {:noreply, %{state | buffer: buffer}}
  end

  def handle_info(:send_ping, %{socket: nil} = state) do
    Logger.warn("Connection is not ready")

    Process.send_after(self(), :send_ping, @ping_interval)
    {:noreply, state}
  end

  def handle_info(:send_ping, %{socket: socket} = state) do
    Logger.debug("Sending CommandPing on broker")

    case :gen_tcp.send(socket, encode_command(CommandPing.new())) do
      :ok ->
        Logger.debug("Sent CommandPing on broker")

        :telemetry.execute(
          [:pulsar_ex, :connection, :ping, :request, :success],
          %{count: 1},
          state
        )

        Process.send_after(self(), :send_ping, @ping_interval)
        {:noreply, state}

      {:error, err} ->
        Logger.error("Error sending CommandPing on broker, #{inspect(err)}")

        :telemetry.execute(
          [:pulsar_ex, :connection, :ping, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, state}
    end
  end

  def handle_info(:send_pong, %{socket: nil} = state) do
    Logger.error("Connection is not ready")

    {:noreply, state}
  end

  def handle_info(:send_pong, %{socket: socket} = state) do
    Logger.debug("Sending CommandPong on broker")

    case :gen_tcp.send(socket, encode_command(CommandPong.new())) do
      :ok ->
        Logger.debug("Sent CommandPong on broker")

        :telemetry.execute(
          [:pulsar_ex, :connection, :pong, :request, :success],
          %{count: 1},
          state
        )

        {:noreply, state}

      {:error, err} ->
        Logger.debug("Error sending CommandPong on broker, #{inspect(err)}")

        :telemetry.execute(
          [:pulsar_ex, :connection, :pong, :request, :error],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, state}
    end
  end

  def handle_info(:tick, %{requests: requests} = state) do
    {timeouts, requests} =
      Enum.split_with(requests, fn {_key, {_from, {_send_ts, deadline}, _request}} ->
        System.monotonic_time(:millisecond) >= deadline
      end)

    Enum.each(timeouts, &reply_error(&1, :timeout))

    :telemetry.execute(
      [:pulsar_ex, :connection, :requests, :timeout],
      %{count: Enum.count(timeouts)},
      state
    )

    :telemetry.execute(
      [:pulsar_ex, :connection, :requests],
      %{size: Enum.count(requests)},
      state
    )

    Process.send_after(self(), :tick, @tick_interval)
    {:noreply, %{state | requests: Enum.into(requests, %{})}}
  end

  # ================== terminate =====================
  @impl true
  def terminate(:normal, %{socket: socket, requests: requests} = state) do
    Logger.debug("Closing connection from broker")

    if socket do
      :gen_tcp.close(socket)
    end

    Enum.each(requests, &reply_error(&1, :closed))

    state
  end

  def terminate(:shutdown, %{socket: socket, requests: requests} = state) do
    Logger.debug("Closing connection from broker")

    if socket do
      :gen_tcp.close(socket)
    end

    Enum.each(requests, &reply_error(&1, :closed))

    state
  end

  def terminate({:shutdown, err}, %{socket: socket, requests: requests} = state) do
    Logger.error("Closing connection from broker, #{inspect(err)}")

    if socket do
      :gen_tcp.close(socket)
    end

    Enum.each(requests, &reply_error(&1, err))

    state
  end

  def terminate(reason, %{socket: socket, requests: requests} = state) do
    Logger.error("Existing connection from broker, #{inspect(reason)}")

    if socket do
      :gen_tcp.close(socket)
    end

    Enum.each(requests, &reply_error(&1, :exit))

    :telemetry.execute(
      [:pulsar_ex, :connection, :exit],
      %{count: 1},
      state
    )

    state
  end

  # ================== handle_command =====================
  defp handle_command(%CommandPing{} = command, _, state) do
    Logger.debug("Received CommandPing from broker, #{inspect(command)}")

    :telemetry.execute(
      [:pulsar_ex, :connection, :ping, :received],
      %{count: 1},
      state
    )

    Process.send(self(), :send_pong, [])
    state
  end

  defp handle_command(%CommandPong{} = command, _, state) do
    Logger.debug("Received CommandPong from broker, #{inspect(command)}")

    :telemetry.execute(
      [:pulsar_ex, :connection, :pong, :received],
      %{count: 1},
      state
    )

    state
  end

  # we don't want to handle consumer message here, we will bundle them to consumers
  defp handle_command(%CommandMessage{}, _, state), do: state

  defp handle_command(
         %CommandCloseProducer{producer_id: producer_id} = command,
         _,
         %{cluster: %Cluster{cluster_name: cluster_name}} = state
       ) do
    Logger.warn(
      "Received CommandCloseProducer for producer [#{producer_id}] from broker, #{inspect(command)}"
    )

    :telemetry.execute(
      [:pulsar_ex, :connection, :close_producer, :received],
      %{count: 1},
      state
    )

    with [{pid, _}] <- Registry.lookup(ProducerIDRegistry, {cluster_name, producer_id}) do
      Producer.close(pid)
    end

    state
  end

  defp handle_command(
         %CommandCloseConsumer{consumer_id: consumer_id} = command,
         _,
         %{cluster: %Cluster{cluster_name: cluster_name}} = state
       ) do
    Logger.warn(
      "Received CloseConsumer for consumer [#{consumer_id}] from broker, #{inspect(command)}"
    )

    :telemetry.execute(
      [:pulsar_ex, :connection, :close_consumer, :received],
      %{count: 1},
      state
    )

    with [{pid, _}] <- Registry.lookup(ConsumerIDRegistry, {cluster_name, consumer_id}) do
      Consumer.close(pid)
    end

    state
  end

  defp handle_command(
         %CommandProducerSuccess{
           producer_ready: true,
           request_id: request_id,
           producer_name: producer_name,
           last_sequence_id: last_sequence_id
         } = response,
         _,
         %{max_message_size: max_message_size, requests: requests} = state
       ) do
    with {{from, {send_ts, _deadline},
           %{
             producer_id: producer_id,
             topic: topic_name,
             producer_access_mode: producer_access_mode,
             metadata: metadata
           }},
          requests} <-
           Map.pop(requests, {:request_id, request_id}) do
      duration = System.monotonic_time() - send_ts
      duration_ms = div(duration, 1_000_000)

      Logger.debug(
        "Created producer [#{producer_id}] for topic [#{topic_name}] on broker after #{duration_ms}ms, #{inspect(response)}"
      )

      reply = %{
        producer_name: producer_name,
        last_sequence_id: last_sequence_id,
        max_message_size: max_message_size,
        producer_access_mode: producer_access_mode,
        properties: from_kv(metadata)
      }

      GenServer.reply(from, {:ok, reply})

      :telemetry.execute(
        [:pulsar_ex, :connection, :producer_ready, :received],
        %{count: 1, duration: duration},
        state
      )

      %{state | requests: requests}
    else
      _ ->
        Logger.error("Received missing CommandProducerSuccess from broker, #{inspect(response)}")

        :telemetry.execute(
          [:pulsar_ex, :connection, :producer_ready, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandProducerSuccess{request_id: request_id} = response,
         _,
         %{requests: requests} = state
       ) do
    with {_, {send_ts, _deadline}, %{producer_id: producer_id, topic: topic_name}} <-
           Map.get(requests, {:request_id, request_id}) do
      duration = System.monotonic_time() - send_ts
      duration_ms = div(duration, 1_000_000)

      Logger.warn(
        "Producer [#{producer_id}] for topic [#{topic_name}] not ready on broker, after #{duration_ms}ms, #{inspect(response)}"
      )

      :telemetry.execute(
        [:pulsar_ex, :connection, :producer_not_ready, :received],
        %{count: 1, duration: duration},
        state
      )

      state
    else
      _ ->
        Logger.error("Received missing CommandProducerSuccess from broker, #{inspect(response)}")

        :telemetry.execute(
          [:pulsar_ex, :connection, :producer_not_ready, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandSuccess{request_id: request_id} = response,
         _,
         %{requests: requests} = state
       ) do
    {request_info, requests} = Map.pop(requests, {:request_id, request_id})
    state = %{state | requests: requests}

    case request_info do
      {from, {send_ts, _deadline},
       %CommandSubscribe{
         consumer_id: consumer_id,
         topic: topic_name,
         consumer_name: consumer_name,
         subscription: subscription,
         subType: sub_type,
         priority_level: priority_level,
         read_compacted: read_compacted,
         initialPosition: initial_position,
         metadata: metadata
       }} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.debug(
          "Created consumer for consumer [#{consumer_id}] subscribing to topic [#{topic_name}] with subscription [#{subscription}] in [#{sub_type}] mode from broker, after #{duration_ms}ms, #{inspect(response)}"
        )

        reply = %{
          consumer_name: consumer_name,
          subscription_type: sub_type,
          priority_level: priority_level,
          read_compacted: read_compacted,
          initial_position: initial_position,
          properties: from_kv(metadata)
        }

        GenServer.reply(from, {:ok, reply})

        :telemetry.execute(
          [:pulsar_ex, :connection, :consumer_ready, :received],
          %{count: 1, duration: duration},
          state
        )

        state

      {nil, {send_ts, _deadline}, %CommandCloseProducer{producer_id: producer_id}} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.debug(
          "Closed producer for producer [#{producer_id}] from broker, after #{duration_ms}ms, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :close_producer, :success],
          %{count: 1},
          state
        )

        state

      {nil, {send_ts, _deadline}, %CommandCloseConsumer{consumer_id: consumer_id}} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.debug(
          "Closed consumer for consumer [#{consumer_id}] closing from broker, after #{duration_ms}ms, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :close_consumer, :success],
          %{count: 1},
          state
        )

        state

      nil ->
        Logger.error("Received missing CommandSuccess from broker, #{inspect(response)}")

        :telemetry.execute(
          [:pulsar_ex, :connection, :command_success, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandError{request_id: request_id, error: err} = response,
         _,
         %{requests: requests} = state
       ) do
    {request_info, requests} = Map.pop(requests, {:request_id, request_id})
    state = %{state | requests: requests}

    case request_info do
      {from, {send_ts, _deadline}, %CommandProducer{producer_id: producer_id, topic: topic_name}} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.error(
          "Error creating producer for producer [#{producer_id}] for topic [#{topic_name}] from broker, after #{duration_ms}ms, #{inspect(response)}"
        )

        GenServer.reply(from, {:error, err})

        :telemetry.execute(
          [:pulsar_ex, :connection, :create_producer, :error],
          %{count: 1, duration: duration},
          state
        )

        state

      {from, {send_ts, _deadline},
       %CommandSubscribe{
         consumer_id: consumer_id,
         topic: topic_name,
         subscription: subscription,
         subType: sub_type
       }} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.error(
          "Error creating consumer for consumer [#{consumer_id}] subscribing to topic [#{topic_name}] with subscription [#{subscription}] in [#{sub_type}] mode from broker, after #{duration_ms}ms, #{inspect(response)}"
        )

        GenServer.reply(from, {:error, err})

        :telemetry.execute(
          [:pulsar_ex, :connection, :subscribe, :error],
          %{count: 1, duration: duration},
          state
        )

        state

      {nil, {send_ts, _deadline}, %CommandCloseProducer{producer_id: producer_id}} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.error(
          "Error closing producer for producer [#{producer_id}] from broker, after #{duration_ms}ms, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :close_producer, :error],
          %{count: 1, duration: duration},
          state
        )

        state

      {nil, {send_ts, _deadline}, %CommandCloseConsumer{consumer_id: consumer_id}} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.error(
          "Error closing consumer for consumer [#{consumer_id}] from broker, after #{duration_ms}ms, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :close_consumer, :error],
          %{count: 1, duration: duration},
          state
        )

        state

      nil ->
        Logger.error("Received missing CommandError from broker, #{inspect(response)}")

        :telemetry.execute(
          [:pulsar_ex, :connection, :command_error, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandSendReceipt{
           producer_id: producer_id,
           sequence_id: sequence_id,
           message_id: message_id
         } = response,
         _,
         %{requests: requests} = state
       ) do
    with {{{pid, _}, {send_ts, _deadline}, _}, requests} <-
           Map.pop(requests, {:sequence_id, producer_id, sequence_id}) do
      duration = System.monotonic_time() - send_ts
      duration_ms = div(duration, 1_000_000)

      Logger.debug(
        "Received CommandSendReceipt for producer [#{producer_id}] from broker, after #{duration_ms}ms, #{inspect(response)}"
      )

      Producer.send_response(pid, {:ok, %{sequence_id: sequence_id, message_id: message_id}})

      :telemetry.execute(
        [:pulsar_ex, :connection, :send_success, :received],
        %{count: 1, duration: duration},
        state
      )

      %{state | requests: requests}
    else
      _ ->
        Logger.error(
          "Received missing CommandSendReceipt for producer [#{producer_id}] from broker, #{inspect({response})}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :send_success, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandSendError{producer_id: producer_id, sequence_id: sequence_id, error: err} =
           response,
         _,
         %{requests: requests} = state
       ) do
    with {{{pid, _}, {send_ts, _deadline}, _}, requests} <-
           Map.pop(requests, {:sequence_id, producer_id, sequence_id}) do
      duration = System.monotonic_time() - send_ts
      duration_ms = div(duration, 1_000_000)

      Logger.error(
        "Received CommandSendError with error for producer [#{producer_id}] from broker, after #{duration_ms}ms, #{inspect(response)}"
      )

      Producer.send_response(pid, {:error, %{sequence_id: sequence_id, error: err}})

      :telemetry.execute(
        [:pulsar_ex, :connection, :send_error, :received],
        %{count: 1, duration: duration},
        state
      )

      %{state | requests: requests}
    else
      _ ->
        Logger.error(
          "Received missing CommandSendError with error for producer [#{producer_id}] from broker, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :send_error, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandAckResponse{request_id: request_id, consumer_id: consumer_id, error: nil} =
           response,
         _,
         %{requests: requests} = state
       ) do
    with {{{pid, _}, {send_ts, _deadline}, %{message_id: message_ids}}, requests} <-
           Map.pop(requests, {:request_id, request_id}) do
      duration = System.monotonic_time() - send_ts
      duration_ms = div(duration, 1_000_000)

      Logger.debug(
        "Received CommandAckResponse for consumer [#{consumer_id}] from broker, #{duration_ms}ms, #{inspect(response)}"
      )

      message_ids =
        message_ids
        |> Enum.map(fn %{ledgerId: ledgerId, entryId: entryId} ->
          {ledgerId, entryId}
        end)

      Consumer.ack_response(pid, {:ok, %{message_ids: message_ids}})

      :telemetry.execute(
        [:pulsar_ex, :connection, :ack_success, :received],
        %{count: 1, duration: duration},
        state
      )

      %{state | requests: requests}
    else
      _ ->
        Logger.error(
          "Received missing CommandAckResponse for consumer [#{consumer_id}] from broker, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :ack_success, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandAckResponse{request_id: request_id, consumer_id: consumer_id, error: err} =
           response,
         _,
         %{requests: requests} = state
       ) do
    with {{{pid, _}, {send_ts, _deadline}, %{message_id: message_ids}}, requests} <-
           Map.pop(requests, {:request_id, request_id}) do
      duration = System.monotonic_time() - send_ts
      duration_ms = div(duration, 1_000_000)

      Logger.error(
        "Received CommandAckResponse with error for consumer [#{consumer_id}] from broker, after #{duration_ms}ms, #{inspect(response)}"
      )

      message_ids =
        message_ids
        |> Enum.map(fn %{ledgerId: ledgerId, entryId: entryId} ->
          {ledgerId, entryId}
        end)

      Consumer.ack_response(pid, {:error, %{message_ids: message_ids, error: err}})

      :telemetry.execute(
        [:pulsar_ex, :connection, :ack_error, :received],
        %{count: 1, duration: duration},
        state
      )

      %{state | requests: requests}
    else
      _ ->
        Logger.error(
          "Received missing CommandAckResponse with error for consumer [#{consumer_id}] from broker, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :ack_error, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandPartitionedTopicMetadataResponse{
           request_id: request_id,
           partitions: partitions,
           response: :Success
         } = response,
         _,
         %{requests: requests} = state
       ) do
    {request_info, requests} = Map.pop(requests, {:request_id, request_id})
    state = %{state | requests: requests}

    case request_info do
      {from, {send_ts, _deadline}, %{topic: topic_name}} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.debug(
          "Received CommandPartitionedTopicMetadataResponse for topic [#{topic_name}] from broker, after #{duration_ms}ms, #{inspect(response)}"
        )

        GenServer.reply(from, {:ok, partitions})

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_partitions_success, :received],
          %{count: 1, duration: duration},
          state
        )

        state

      nil ->
        Logger.error(
          "Received missing CommandPartitionedTopicMetadataResponse from broker, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_partitions_success, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandPartitionedTopicMetadataResponse{
           request_id: request_id,
           response: :Failed,
           error: err
         } = response,
         _,
         %{requests: requests} = state
       ) do
    {request_info, requests} = Map.pop(requests, {:request_id, request_id})
    state = %{state | requests: requests}

    case request_info do
      {from, {send_ts, _deadline}, _request} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.error(
          "Received CommandPartitionedTopicMetadataResponse with error from broker, after #{duration_ms}ms, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_partitions_error, :received],
          %{count: 1, duration: duration},
          state
        )

        GenServer.reply(from, {:error, err})

        state

      nil ->
        Logger.error(
          "Received missing CommandPartitionedTopicMetadataResponse with error from broker, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_partitions_error, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandLookupTopicResponse{
           request_id: request_id,
           brokerServiceUrl: broker_url,
           response: :Connect
         } = response,
         _,
         %{requests: requests} = state
       ) do
    {request_info, requests} = Map.pop(requests, {:request_id, request_id})
    state = %{state | requests: requests}

    case request_info do
      {from, {send_ts, _deadline}, %{topic: topic_name}} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.debug(
          "Received CommandLookupTopicResponse from broker for topic [#{topic_name}], after #{duration_ms}ms, #{inspect(response)}"
        )

        GenServer.reply(from, {:connect, broker_url})

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_connect, :received],
          %{count: 1, duration: duration},
          state
        )

        state

      nil ->
        Logger.error(
          "Received missing CommandLookupTopicResponse from broker, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_connect, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandLookupTopicResponse{
           request_id: request_id,
           authoritative: authoritative,
           brokerServiceUrl: broker_url,
           response: :Redirect
         } = response,
         _,
         %{requests: requests} = state
       ) do
    {request_info, requests} = Map.pop(requests, {:request_id, request_id})
    state = %{state | requests: requests}

    case request_info do
      {from, {send_ts, _deadline}, %{topic: topic_name}} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.debug(
          "Received CommandLookupTopicResponse for topic [#{topic_name}] from broker, after #{duration_ms}ms, #{inspect(response)}"
        )

        GenServer.reply(from, {:redirect, broker_url, authoritative})

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_redirect, :received],
          %{count: 1, duration: duration},
          state
        )

        state

      nil ->
        Logger.error(
          "Received missing CommandLookupTopicResponse from broker, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_redirect, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(
         %CommandLookupTopicResponse{request_id: request_id, response: :Failed, error: err} =
           response,
         _,
         %{requests: requests} = state
       ) do
    {request_info, requests} = Map.pop(requests, {:request_id, request_id})
    state = %{state | requests: requests}

    case request_info do
      {from, {send_ts, _deadline}, _request} ->
        duration = System.monotonic_time() - send_ts
        duration_ms = div(duration, 1_000_000)

        Logger.error(
          "Received CommandLookupTopicResponse with error from broker, after #{duration_ms}ms, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_error, :received],
          %{count: 1, duration: duration},
          state
        )

        GenServer.reply(from, {:error, err})

        state

      nil ->
        Logger.error(
          "Received missing CommandLookupTopicResponse with error from broker, #{inspect(response)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :connection, :lookup_topic_error, :missing],
          %{count: 1},
          state
        )

        state
    end
  end

  defp handle_command(command, _payload, state) do
    Logger.warn("Received UnknownCommand from broker, #{inspect(command)}")

    :telemetry.execute(
      [:pulsar_ex, :connection, :unknown_command, :received],
      %{count: 1},
      state
    )

    state
  end

  # ================== private =====================
  defp do_connect(host, port, cluster_opts) do
    socket_opts = Keyword.get(cluster_opts, :socket_opts, []) |> optimize_socket_opts()
    connection_timeout = Keyword.get(cluster_opts, :connection_timeout, @connection_timeout)
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

  defp get_request_id(%{last_request_id: last_request_id} = state) do
    request_id = last_request_id + 1
    {request_id, %{state | last_request_id: request_id}}
  end

  defp reply_error(
         {{:sequence_id, _producer_id, sequence_id}, {{pid, _}, {_send_ts, _deadline}, _request}},
         err
       ) do
    Producer.send_response(pid, {:error, %{sequence_id: sequence_id, error: err}})
  end

  defp reply_error(
         {_, {{pid, _}, {_send_ts, _deadline}, %CommandAck{message_id: message_ids}}},
         err
       ) do
    message_ids =
      message_ids
      |> Enum.map(fn %{ledgerId: ledgerId, entryId: entryId} ->
        {ledgerId, entryId}
      end)

    Consumer.ack_response(pid, {:error, %{message_ids: message_ids, error: err}})
  end

  defp reply_error({_, {nil, {_send_ts, _deadline}, _request}}, _err), do: nil

  defp reply_error({_, {from, {_send_ts, _deadline}, _request}}, err) do
    GenServer.reply(from, {:error, err})
  end
end
