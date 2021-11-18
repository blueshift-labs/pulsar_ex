defmodule PulsarEx do
  alias PulsarEx.{
    ProducerManager,
    PartitionManager,
    PartitionedProducer,
    Partitioner,
    Topic,
    ConsumerManager
  }

  def produce(topic_name, payload, message_opts \\ [], producer_opts \\ [])

  def produce(topic_name, payload, message_opts, producer_opts)
      when is_map(message_opts) or is_map(producer_opts) do
    produce(topic_name, payload, Enum.into(message_opts, []), Enum.into(producer_opts, []))
  end

  def produce(topic_name, payload, message_opts, producer_opts) do
    with {:ok, {%Topic{partition: nil}, partitions}} <- PartitionManager.lookup(topic_name) do
      partition =
        message_opts
        |> Keyword.get(:partition_key)
        |> Partitioner.assign(partitions)

      with {:ok, producer} <- ProducerManager.get_producer(topic_name, partition, producer_opts) do
        PartitionedProducer.produce(producer, payload, message_opts)
      end
    else
      {:ok, {%Topic{}, _}} -> {:error, :partitioned_topic}
      err -> err
    end
  end

  defdelegate start_consumer(tenant, namespace, regex, subscription, module, opts),
    to: ConsumerManager

  defdelegate start_consumer(topic_name, subscription, module, opts), to: ConsumerManager

  defdelegate start_consumer(topic_name, partitions, subscription, module, opts),
    to: ConsumerManager

  defdelegate stop_consumer(tenant, namespace, regex, subscription), to: ConsumerManager

  defdelegate stop_consumer(topic_name, subscription), to: ConsumerManager
  defdelegate stop_consumer(topic_name, partitions, subscription), to: ConsumerManager
end
