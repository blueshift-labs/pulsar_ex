defmodule PulsarEx.TestConnection do
  use GenServer

  alias PulsarEx.Connection.{LookupCallback, ProducerCallback, ConsumerCallback}
  alias PulsarEx.Proto.MessageIdData
  alias PulsarEx.{Producer, Consumer, ProducerMessage, ConsumerMessage, ConsumerIDRegistry}

  @behaviour LookupCallback
  @behaviour ProducerCallback
  @behaviour ConsumerCallback

  def start_link({cluster, broker}) do
    GenServer.start_link(__MODULE__, {cluster, broker}, name: __MODULE__)
  end

  @impl LookupCallback
  def lookup_topic_partitions(conn, topic_name, deadline) do
    GenServer.call(conn, {:lookup_topic_partitions, topic_name, deadline})
  end

  @impl LookupCallback
  def lookup_topic(conn, topic_name, authoritative, deadline) do
    GenServer.call(conn, {:lookup_topic, topic_name, authoritative, deadline})
  end

  @impl ProducerCallback
  def create_producer(conn, producer_id, topic_name, producer_opts, deadline) do
    GenServer.call(conn, {:create_producer, producer_id, topic_name, producer_opts, deadline})
  end

  @impl ProducerCallback
  def send_message(conn, producer_id, sequence_id, message, deadline) do
    GenServer.call(conn, {:send, producer_id, sequence_id, message, deadline})
  end

  @impl ProducerCallback
  def send_messages(conn, producer_id, sequence_id, messages, deadline) when is_list(messages) do
    GenServer.call(conn, {:send, producer_id, sequence_id, messages, deadline})
  end

  @impl ProducerCallback
  def close_producer(conn, producer_id) do
    GenServer.cast(conn, {:close_producer, producer_id})
  end

  @impl ConsumerCallback
  def subscribe(conn, consumer_id, topic_name, subscription, sub_type, consumer_opts, deadline) do
    GenServer.call(
      conn,
      {:subscribe, consumer_id, topic_name, subscription, sub_type, consumer_opts, deadline}
    )
  end

  @impl ConsumerCallback
  def ack(conn, consumer_id, ack_type, msg_ids, deadline) do
    GenServer.call(conn, {:ack, consumer_id, ack_type, msg_ids, deadline})
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

  def producer_messages(topic_name) do
    GenServer.call(__MODULE__, {:producer_messages, topic_name})
  end

  def consumer_messages(topic_name) do
    GenServer.call(__MODULE__, {:consumer_messages, topic_name})
  end

  @impl true
  def init({cluster, broker}) do
    state = %{
      cluster: cluster,
      broker: broker,
      producers: %{},
      consumers: %{},
      producer_messages: %{},
      consumer_messages: %{}
    }

    Process.send_after(self(), :deliver, 10)

    {:ok, state}
  end

  @impl true
  def handle_call({:lookup_topic_partitions, _topic_name, _deadline}, _from, state) do
    {:reply, {:ok, 0}, state}
  end

  def handle_call(
        {:lookup_topic, _topic_name, _authoritative, _deadline},
        _from,
        %{broker: broker} = state
      ) do
    {:reply, {:connect, to_string(broker)}, state}
  end

  def handle_call(
        {:create_producer, producer_id, topic_name, _producer_opts, _deadline},
        _from,
        %{producers: producers} = state
      ) do
    producers = Map.put(producers, producer_id, topic_name)

    reply = %{
      producer_name: "test",
      last_sequence_id: 0,
      max_message_size: 1000,
      producer_access_mode: :Shared,
      properties: %{}
    }

    {:reply, {:ok, reply}, %{state | producers: producers}}
  end

  def handle_call(
        {:send, _producer_id, _sequence_id, %ProducerMessage{payload: "error"}, _deadline},
        _from,
        state
      ) do
    {:reply, {:error, :test}, state}
  end

  def handle_call(
        {:send, _producer_id, _sequence_id, %ProducerMessage{payload: "close"}, _deadline},
        {pid, _} = _from,
        state
      ) do
    Producer.close(pid)
    {:reply, :ok, state}
  end

  def handle_call(
        {:send, producer_id, sequence_id, data, _deadline},
        from,
        %{producers: producers, producer_messages: producer_messages} = state
      ) do
    timeout =
      case data do
        %ProducerMessage{payload: "instant-" <> _} -> 0
        %ProducerMessage{payload: "timeout"} -> 5000
        [%ProducerMessage{payload: "timeout"} | _] -> 5000
        _ -> 1000
      end

    topic_name = Map.fetch!(producers, producer_id)

    msgs = Map.get(producer_messages, topic_name, [])
    msgs = [{sequence_id, data} | msgs]

    Process.send_after(self(), {:send_response, from, sequence_id}, timeout)

    {:reply, :ok, %{state | producer_messages: Map.put(producer_messages, topic_name, msgs)}}
  end

  def handle_call(
        {:subscribe, consumer_id, topic_name, _subscription, _sub_type, consumer_opts, _deadline},
        _from,
        %{consumers: consumers, consumer_messages: consumer_messages} = state
      ) do
    notify = Keyword.fetch!(consumer_opts, :notify)
    message_count = Keyword.get(consumer_opts, :message_count, 100)
    consumers = Map.put(consumers, consumer_id, {topic_name, notify, 0})

    consumer_messages =
      unless Map.get(consumer_messages, topic_name) do
        messages =
          Enum.map(1..message_count, fn idx ->
            %ConsumerMessage{
              message_id: MessageIdData.new(ledgerId: 2, entryId: idx),
              batch_index: 0,
              batch_size: 1,
              producer_name: "test",
              sequence_id: idx,
              publish_time: Timex.now(),
              properties: %{},
              partition_key: "partition_key",
              event_time: Timex.now(),
              ordering_key: "ordering_key",
              deliver_at_time: Timex.now(),
              redelivery_count: 0,
              payload: "#{idx}"
            }
          end)

        Map.put(consumer_messages, topic_name, {[], messages})
      else
        consumer_messages
      end

    reply = %{
      consumer_name: "test",
      subscription_type: :Shared,
      priority_level: 1,
      read_compacted: false,
      initial_position: Latest,
      properties: %{}
    }

    {:reply, {:ok, reply}, %{state | consumers: consumers, consumer_messages: consumer_messages}}
  end

  def handle_call(
        {:ack, consumer_id, _ack_type, msg_ids, _deadline},
        {pid, _} = _from,
        %{consumers: consumers, consumer_messages: consumer_messages} = state
      ) do
    {topic_name, {notify, condition}, _permits} = Map.fetch!(consumers, consumer_id)
    {delivered, messages} = Map.fetch!(consumer_messages, topic_name)

    acks = MapSet.new(msg_ids)

    delivered =
      Enum.reject(delivered, fn %{message_id: %{ledgerId: ledger_id, entryId: entry_id}} ->
        MapSet.member?(acks, {ledger_id, entry_id})
      end)

    consumer_messages = Map.put(consumer_messages, topic_name, {delivered, messages})
    Consumer.ack_response(pid, {:ok, %{message_ids: msg_ids}})

    cond do
      condition == :when_done && messages == [] && delivered == [] ->
        Process.send(notify, :done, [])

      true ->
        nil
    end

    {:reply, :ok, %{state | consumer_messages: consumer_messages}}
  end

  def handle_call({:flow_permits, consumer_id, permits}, _from, %{consumers: consumers} = state) do
    {topic_name, {notify, condition}, p} = Map.fetch!(consumers, consumer_id)
    consumers = Map.put(consumers, consumer_id, {topic_name, {notify, condition}, p + permits})

    {:reply, :ok, %{state | consumers: consumers}}
  end

  def handle_call(
        {:redeliver, consumer_id, msg_ids},
        _from,
        %{consumers: consumers, consumer_messages: consumer_messages} = state
      ) do
    {topic_name, {_notify, _condition}, _permits} = Map.fetch!(consumers, consumer_id)

    {delivered, messages} = Map.fetch!(consumer_messages, topic_name)

    nacks = MapSet.new(msg_ids)

    {put_back, delivered} =
      Enum.split_with(delivered, fn %{message_id: %{ledgerId: ledger_id, entryId: entry_id}} ->
        MapSet.member?(nacks, {ledger_id, entry_id})
      end)

    put_back =
      Enum.map(put_back, fn %{redelivery_count: redelivery_count} = message ->
        %{message | redelivery_count: redelivery_count + 1}
      end)

    messages = put_back ++ messages
    consumer_messages = Map.put(consumer_messages, topic_name, {delivered, messages})

    {:reply, :ok, %{state | consumer_messages: consumer_messages}}
  end

  def handle_call(
        {:producer_messages, topic_name},
        _from,
        %{producer_messages: producer_messages} = state
      ) do
    {:reply, Map.get(producer_messages, topic_name, []) |> Enum.reverse(), state}
  end

  def handle_call(
        {:consumer_messages, topic_name},
        _from,
        %{consumer_messages: consumer_messages} = state
      ) do
    {:reply, Map.get(consumer_messages, topic_name), state}
  end

  @impl true
  def handle_cast({:close_producer, _producer_id}, state) do
    {:noreply, state}
  end

  def handle_cast({:close_consumer, _consumer_id}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:send_response, {pid, _}, sequence_id}, state) do
    Producer.send_response(
      pid,
      {:ok,
       %{
         sequence_id: sequence_id,
         message_id: MessageIdData.new(ledgerId: 1, entryId: sequence_id)
       }}
    )

    {:noreply, state}
  end

  def handle_info(
        :deliver,
        %{
          consumers: consumers,
          consumer_messages: consumer_messages,
          cluster: %{cluster_name: cluster_name}
        } = state
      ) do
    result =
      Enum.reduce(consumers, [], fn {consumer_id, {topic_name, {notify, condition}, permits}},
                                    acc ->
        with [{pid, _}] <- Registry.lookup(ConsumerIDRegistry, {cluster_name, consumer_id}) do
          {delivered, messages} = Map.fetch!(consumer_messages, topic_name)

          if permits <= length(messages) do
            {taken, messages} = Enum.split(messages, permits)
            delivered = delivered ++ taken

            if taken != [] do
              Consumer.messages(pid, taken)
            end

            [{consumer_id, topic_name, notify, condition, 0, delivered, messages} | acc]
          else
            permits = permits - length(messages)
            taken = messages
            delivered = delivered ++ taken
            messages = []

            if taken != [] do
              Consumer.messages(pid, taken)
            end

            [{consumer_id, topic_name, notify, condition, permits, delivered, messages} | acc]
          end
        else
          _ -> acc
        end
      end)

    consumers =
      Enum.reduce(result, consumers, fn {consumer_id, topic_name, notify, condition, permits,
                                         _delivered, _messages},
                                        acc ->
        Map.put(acc, consumer_id, {topic_name, {notify, condition}, permits})
      end)

    consumer_messages =
      Enum.reduce(result, consumer_messages, fn {_consumer_id, topic_name, _notify, _condition,
                                                 _permits, delivered, messages},
                                                acc ->
        Map.put(acc, topic_name, {delivered, messages})
      end)

    Process.send_after(self(), :deliver, 10)
    {:noreply, %{state | consumers: consumers, consumer_messages: consumer_messages}}
  end
end
