defmodule PulsarEx do
  alias PulsarEx.{
    Topic,
    Message,
    Partitioner,
    ProducerManager,
    PartitionManager,
    Producer,
    RetryStrategy,
    ConsumerRegistry,
    ConsumerManager
  }

  alias PulsarEx.RetryStrategy.ExponentialRetry
  alias PulsarEx.Proto.MessageIdData

  defmodule ProducerImpl do
    @behaviour PulsarEx.ProducerCallback

    @impl true
    def produce(cluster_name, topic_name, payload, message_opts, %{} = producer_opts) do
      produce(cluster_name, topic_name, payload, message_opts, Enum.into(producer_opts, []))
    end

    def produce(cluster_name, topic_name, payload, %{} = message_opts, producer_opts) do
      produce(cluster_name, topic_name, payload, Enum.into(message_opts, []), producer_opts)
    end

    def produce(cluster_name, topic_name, payload, message_opts, producer_opts) do
      {retry_strategy, producer_opts} =
        Keyword.pop(producer_opts, :retry_strategy, ExponentialRetry)

      default_options = retry_strategy.default_options()

      options =
        default_options
        |> Keyword.merge(producer_opts)
        |> Keyword.take(Keyword.keys(default_options))
        |> Keyword.merge(cluster: cluster_name, topic: topic_name)

      RetryStrategy.run_with_retrying([retry_strategy: retry_strategy] ++ options, fn ->
        do_produce(cluster_name, topic_name, payload, message_opts, producer_opts)
      end)
    end

    defp do_produce(cluster_name, topic_name, payload, message_opts, producer_opts) do
      cluster_name = "#{cluster_name}"

      with {:ok, {%Topic{partition: nil} = topic, partitions}} <-
             PartitionManager.lookup(cluster_name, topic_name) do
        partition =
          message_opts
          |> Keyword.get(:partition_key)
          |> Partitioner.assign(partitions)

        topic = %{topic | partition: partition}

        {timeout, message_opts} = Keyword.pop(message_opts, :timeout)

        with {:ok, producer} <- ProducerManager.get_producer(cluster_name, topic, producer_opts) do
          message_opts = map_message_opts(message_opts)
          message = create_message(topic, payload, message_opts)

          if timeout do
            Producer.produce(producer, message, System.monotonic_time(:millisecond) + timeout)
          else
            Producer.produce(producer, message)
          end
        end
      else
        {:ok, {%Topic{}, _}} -> {:error, :partitioned_topic}
        err -> err
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

    defp create_message(%Topic{} = topic, payload, message_opts) when is_list(payload) do
      Enum.map(payload, &create_message(topic, &1, message_opts))
    end

    defp create_message(%Topic{} = topic, payload, message_opts) do
      %Message{
        topic: to_string(topic),
        payload: payload,
        properties: Map.get(message_opts, :properties),
        partition_key: Map.get(message_opts, :partition_key),
        ordering_key: Map.get(message_opts, :ordering_key),
        event_time: Map.get(message_opts, :event_time),
        deliver_at_time: Map.get(message_opts, :deliver_at_time)
      }
    end
  end

  @doc """
  Publish a message to pulsar, create a pool of producers if not yet created
  """
  @spec produce(String.t(), binary(), keyword, keyword) ::
          {:ok, %MessageIdData{}} | {:error, any()}
  def produce(topic_name, payload, message_opts \\ [], producer_opts \\ [])

  def produce(topic_name, payload, message_opts, producer_opts) do
    producer_module = Application.get_env(:pulsar_ex, :producer_module, ProducerImpl)
    producer_module.produce("default", topic_name, payload, message_opts, producer_opts)
  end

  @doc """
  List all the consumers on default cluster
  """
  def consumers() do
    Registry.select(ConsumerRegistry, [
      {{{:"$1", :"$2", :"$3"}, :"$4", :"$5"}, [], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
    ])
  end

  @doc """
  List all the consumers for cluster
  """
  def consumers(cluster_name) do
    Registry.select(ConsumerRegistry, [
      {{{cluster_name, :"$1", :"$2"}, :"$3", :"$4"}, [],
       [{{cluster_name, :"$1", :"$2", :"$3", :"$4"}}]}
    ])
  end

  @doc """
  List all the consumers for cluster on a specific topic
  """
  def consumers(cluster_name, topic_name) do
    Registry.select(ConsumerRegistry, [
      {{{cluster_name, topic_name, :"$1"}, :"$2", :"$3"}, [],
       [{{cluster_name, topic_name, :"$1", :"$2", :"$3"}}]}
    ])
  end

  @doc """
  List all the consumers for cluster on a specific topic with specific subscription
  """
  def consumers(cluster_name, topic_name, subscription) do
    Registry.select(ConsumerRegistry, [
      {{{cluster_name, topic_name, subscription}, :"$1", :"$2"}, [],
       [{{cluster_name, topic_name, subscription, :"$1", :"$2"}}]}
    ])
  end

  @type tenant :: String.t() | Regex.t()
  @type namespace :: String.t() | Regex.t()
  @type topic :: String.t() | Regex.t()
  @type subscription :: String.t()

  @doc """
  Start consumers with topic pattern
  """
  @spec start_consumer(tenant(), namespace(), topic(), subscription(), module(), keyword()) ::
          :ok | {:error, any()}
  def start_consumer(tenant, namespace, topic, subscription, module, opts) do
    ConsumerManager.start_consumer(
      "default",
      tenant,
      namespace,
      topic,
      subscription,
      module,
      opts
    )
  end

  @type topic_name :: String.t()

  @doc """
  Start consumer from a fully quantified topic
  """
  @spec start_consumer(topic_name(), subscription(), module(), keyword()) ::
          :ok | {:error, any()}
  def start_consumer(topic_name, subscription, module, opts) do
    ConsumerManager.start_consumer("default", topic_name, subscription, module, opts)
  end

  @doc """
  Stop consumers with topic pattern
  """
  @spec stop_consumer(tenant(), namespace(), topic(), subscription()) :: :ok
  def stop_consumer(tenant, namespace, topic, subscription) do
    ConsumerManager.stop_consumer("default", tenant, namespace, topic, subscription)
  end

  @doc """
  Stop consumers with fully quantified topic
  """
  @spec stop_consumer(topic(), subscription()) :: :ok
  def stop_consumer(topic, subscription) do
    ConsumerManager.stop_consumer("default", topic, subscription)
  end
end
