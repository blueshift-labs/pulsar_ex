defmodule PulsarEx do
  alias PulsarEx.{
    ProducerRegistry,
    ProducerSupervisor,
    ProducerManager,
    PartitionedProducer,
    Topic,
    Partitioner,
    ConsumerRegistry,
    ConsumerManager,
    ConsumerSupervisor,
    Consumer,
    ConsumerMessage
  }

  @retry_delay 500
  @max_retry 5

  def sync_produce(topic_name, payload, message_opts \\ [], producer_opts \\ []) do
    retry_produce(true, topic_name, payload, message_opts, producer_opts)
  end

  def async_produce(topic_name, payload, message_opts \\ [], producer_opts \\ []) do
    retry_produce(false, topic_name, payload, message_opts, producer_opts)
  end

  defp retry_produce(sync?, topic_name, payload, message_opts, producer_opts, attempts \\ 0) do
    case produce(sync?, topic_name, payload, message_opts, producer_opts) do
      {:ok, _} = ok ->
        ok

      {:error, _} = err ->
        if attempts >= @max_retry do
          err
        else
          Process.sleep(@retry_delay)
          retry_produce(sync?, topic_name, payload, message_opts, producer_opts, attempts + 1)
        end
    end
  end

  defp produce(sync?, topic_name, payload, message_opts, producer_opts) do
    case ProducerSupervisor.lookup_partitions(topic_name) do
      [] ->
        case ProducerManager.create(topic_name, producer_opts) do
          :ok ->
            produce(sync?, topic_name, payload, message_opts, producer_opts)

          {:error, :already_started} ->
            produce(sync?, topic_name, payload, message_opts, producer_opts)

          {:error, _} = err ->
            err
        end

      [{_, {%Topic{partition: nil} = topic, partitions}}] ->
        partition_key = Keyword.get(message_opts, :partition_key)
        partition = Partitioner.assign(partition_key, partitions)

        case Registry.lookup(ProducerRegistry, %{topic | partition: partition}) do
          [] ->
            {:error, :producer_not_ready}

          [{pool, _}] ->
            # we are just using the pool to do round robin, producing message doesn't have to block the pool
            producer = :poolboy.checkout(pool)
            :poolboy.checkin(pool, producer)

            PartitionedProducer.produce(producer, payload, message_opts, sync?)
        end
    end
  end

  def ack(%ConsumerMessage{} = message) do
    ConsumerMessage.ack(message)
  end

  def nack(%ConsumerMessage{} = message) do
    ConsumerMessage.nack(message)
  end

  def poll(topic_name, subscription, module \\ nil, consumer_opts \\ [])

  def poll(topic_name, subscription, module, consumer_opts) do
    case ConsumerSupervisor.lookup_partitions(topic_name) do
      [] ->
        case ConsumerManager.create(topic_name, subscription, module, consumer_opts) do
          :ok -> poll(topic_name, subscription, module, consumer_opts)
          {:error, :already_started} -> poll(topic_name, subscription, module, consumer_opts)
          {:error, _} = err -> err
        end

      [{_, {%Topic{} = topic, partitions}}] ->
        topic =
          case partitions do
            0 -> topic
            _ -> %{topic | partition: Enum.random(0..(partitions - 1))}
          end

        case Registry.lookup(ConsumerRegistry, {topic, subscription}) do
          [] ->
            {:error, :consumer_not_ready}

          [{pool, _}] ->
            # we are just using the pool to do round robin, consuming message doesn't have to block the pool
            consumer = :poolboy.checkout(pool)
            :poolboy.checkin(pool, consumer)

            Consumer.poll(consumer)
        end
    end
  end
end
