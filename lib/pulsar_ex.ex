defmodule PulsarEx do
  alias PulsarEx.{
    ProducerManager,
    PartitionManager,
    PartitionedProducer,
    Partitioner,
    Topic,
    ConsumerManager,
    ProducerImpl
  }

  defmodule ProducerImpl do
    alias PulsarEx.ProducerCallback
    @behaviour ProducerCallback

    @impl true
    def produce(cluster, topic_name, payload, message_opts, producer_opts) do
      with {:ok, {%Topic{partition: nil}, partitions}} <-
             PartitionManager.lookup(cluster, topic_name) do
        partition =
          message_opts
          |> Keyword.get(:partition_key)
          |> Partitioner.assign(partitions)

        with {:ok, producer} <-
               ProducerManager.get_producer(cluster, topic_name, partition, producer_opts) do
          PartitionedProducer.produce(producer, payload, message_opts)
        end
      else
        {:ok, {%Topic{}, _}} -> {:error, :partitioned_topic}
        err -> err
      end
    end

    @impl true
    def async_produce(cluster, topic_name, payload, message_opts, producer_opts) do
      with {:ok, {%Topic{partition: nil}, partitions}} <-
             PartitionManager.lookup(cluster, topic_name) do
        partition =
          message_opts
          |> Keyword.get(:partition_key)
          |> Partitioner.assign(partitions)

        with {:ok, producer} <-
               ProducerManager.get_producer(cluster, topic_name, partition, producer_opts) do
          PartitionedProducer.async_produce(producer, payload, message_opts)
        end
      else
        {:ok, {%Topic{}, _}} -> {:error, :partitioned_topic}
        err -> err
      end
    end
  end

  defmodule Clusters do
    def produce(cluster, topic_name, payload, message_opts \\ [], producer_opts \\ [])

    def produce(cluster, topic_name, payload, message_opts, producer_opts)
        when is_map(message_opts) or is_map(producer_opts) do
      produce(
        cluster,
        topic_name,
        payload,
        Enum.into(message_opts, []),
        Enum.into(producer_opts, [])
      )
    end

    def produce(cluster, topic_name, payload, message_opts, producer_opts) do
      producer_module = Application.get_env(:pulsar_ex, :producer_module, ProducerImpl)
      producer_module.produce(cluster, topic_name, payload, message_opts, producer_opts)
    end

    def async_produce(
          cluster,
          topic_name,
          payload,
          message_opts \\ [],
          producer_opts \\ []
        )

    def async_produce(cluster, topic_name, payload, message_opts, producer_opts)
        when is_map(message_opts) or is_map(producer_opts) do
      async_produce(
        cluster,
        topic_name,
        payload,
        Enum.into(message_opts, []),
        Enum.into(producer_opts, [])
      )
    end

    def async_produce(cluster, topic_name, payload, message_opts, producer_opts) do
      producer_module = Application.get_env(:pulsar_ex, :producer_module, ProducerImpl)
      producer_module.async_produce(cluster, topic_name, payload, message_opts, producer_opts)
    end

    def consumers(cluster) do
      DynamicSupervisor.which_children(ConsumerManager.consumers(cluster))
    end

    def start_consumer(cluster, tenant, namespace, regex, subscription, module, opts) do
      ConsumerManager.start_consumer(
        cluster,
        tenant,
        namespace,
        regex,
        subscription,
        module,
        opts
      )
    end

    def start_consumer(cluster, topic_name, subscription, module, opts) do
      ConsumerManager.start_consumer(cluster, topic_name, subscription, module, opts)
    end

    def start_consumer(cluster, topic_name, partitions, subscription, module, opts) do
      ConsumerManager.start_consumer(cluster, topic_name, partitions, subscription, module, opts)
    end

    def stop_consumer(cluster, tenant, namespace, regex, subscription) do
      ConsumerManager.stop_consumer(cluster, tenant, namespace, regex, subscription)
    end

    def stop_consumer(cluster, topic_name, subscription) do
      ConsumerManager.stop_consumer(cluster, topic_name, subscription)
    end

    def stop_consumer(cluster, topic_name, partitions, subscription) do
      ConsumerManager.stop_consumer(cluster, topic_name, partitions, subscription)
    end
  end

  def produce(topic_name, payload, message_opts \\ [], producer_opts \\ [])

  def produce(topic_name, payload, message_opts, producer_opts)
      when is_map(message_opts) or is_map(producer_opts) do
    produce(
      topic_name,
      payload,
      Enum.into(message_opts, []),
      Enum.into(producer_opts, [])
    )
  end

  def produce(topic_name, payload, message_opts, producer_opts) do
    producer_module = Application.get_env(:pulsar_ex, :producer_module, ProducerImpl)
    producer_module.produce(:default, topic_name, payload, message_opts, producer_opts)
  end

  def async_produce(
        topic_name,
        payload,
        message_opts \\ [],
        producer_opts \\ []
      )

  def async_produce(topic_name, payload, message_opts, producer_opts)
      when is_map(message_opts) or is_map(producer_opts) do
    async_produce(
      topic_name,
      payload,
      Enum.into(message_opts, []),
      Enum.into(producer_opts, [])
    )
  end

  def async_produce(topic_name, payload, message_opts, producer_opts) do
    producer_module = Application.get_env(:pulsar_ex, :producer_module, ProducerImpl)
    producer_module.async_produce(:default, topic_name, payload, message_opts, producer_opts)
  end

  def consumers() do
    DynamicSupervisor.which_children(ConsumerManager.consumers(:default))
  end

  def start_consumer(tenant, namespace, regex, subscription, module, opts) do
    ConsumerManager.start_consumer(:default, tenant, namespace, regex, subscription, module, opts)
  end

  def start_consumer(topic_name, subscription, module, opts) do
    ConsumerManager.start_consumer(:default, topic_name, subscription, module, opts)
  end

  def start_consumer(topic_name, partitions, subscription, module, opts) do
    ConsumerManager.start_consumer(:default, topic_name, partitions, subscription, module, opts)
  end

  def stop_consumer(tenant, namespace, regex, subscription) do
    ConsumerManager.stop_consumer(:default, tenant, namespace, regex, subscription)
  end

  def stop_consumer(topic_name, subscription) do
    ConsumerManager.stop_consumer(:default, topic_name, subscription)
  end

  def stop_consumer(topic_name, partitions, subscription) do
    ConsumerManager.stop_consumer(:default, topic_name, partitions, subscription)
  end
end
