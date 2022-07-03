defmodule PulsarEx.ProducerManager do
  defmodule State do
    @enforce_keys [
      :cluster,
      :cluster_opts
    ]

    defstruct [
      :cluster,
      :cluster_opts
    ]
  end

  use GenServer

  require Logger

  alias PulsarEx.{ProducerRegistry, PartitionedProducer, PartitionManager, Topic}

  @num_producers 1
  @connection_timeout 60_000

  def producers(cluster), do: String.to_atom("PulsarEx.Producers.#{cluster}")

  def get_producer(cluster, topic_name, partition, producer_opts \\ []) do
    with [] <- Registry.lookup(ProducerRegistry, {cluster, topic_name, partition}),
         {:ok, pool} <-
           GenServer.call(
             name(cluster),
             {:create, topic_name, partition, producer_opts},
             @connection_timeout
           ) do
      {:ok, :poolboy.transaction(pool, & &1)}
    else
      [{pool, _}] ->
        {:ok, :poolboy.transaction(pool, & &1)}

      err ->
        err
    end
  end

  def start_link(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)
    GenServer.start_link(__MODULE__, cluster_opts, name: name(cluster))
  end

  @impl true
  def init(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)
    Logger.debug("Starting producer manager, on cluster #{cluster}")

    producer_opts = Keyword.get(cluster_opts, :producer_opts, [])
    auto_start = Keyword.get(producer_opts, :auto_start, false)

    state = %State{
      cluster: cluster,
      cluster_opts: cluster_opts
    }

    if auto_start do
      Keyword.get(cluster_opts, :producers, [])
      |> Enum.reduce_while(:ok, fn producer_opts, :ok ->
        topic_name = Keyword.fetch!(producer_opts, :topic)

        case start_producers(topic_name, producer_opts, state) do
          :ok -> {:cont, :ok}
          {:error, _} = err -> {:halt, err}
        end
      end)
      |> case do
        :ok -> {:ok, state}
        err -> {:stop, err}
      end
    else
      {:ok, state}
    end
  end

  @impl true
  def handle_call(
        {:create, topic_name, partition, producer_opts},
        _from,
        %{cluster: cluster} = state
      ) do
    with [] <- Registry.lookup(ProducerRegistry, {cluster, topic_name, partition}),
         {:ok, pool} <- start_producer(topic_name, partition, producer_opts, state) do
      {:reply, {:ok, pool}, state}
    else
      [{pool, _}] ->
        {:reply, {:ok, pool}, state}

      err ->
        {:reply, err, state}
    end
  end

  defp start_producer(topic_name, partition, producer_opts, %{cluster: cluster} = state) do
    started =
      DynamicSupervisor.start_child(
        producers(cluster),
        producer_spec(topic_name, partition, producer_opts, state)
      )

    case started do
      {:ok, _} ->
        Logger.debug(
          "Started producer for topic #{topic_name}, partition #{partition}, on cluster #{cluster}"
        )

      {:error, err} ->
        Logger.error(
          "Error starting producer for topic #{topic_name}, partition #{partition}, on cluster #{cluster}, #{inspect(err)}"
        )
    end

    started
  end

  defp producer_opts(producer_opts, cluster_opts) do
    Keyword.merge(
      Keyword.get(cluster_opts, :producer_opts, []),
      producer_opts,
      fn
        :properties, v1, v2 -> Keyword.merge(v1, v2)
        _, _, v -> v
      end
    )
  end

  defp producer_spec(topic_name, partition, producer_opts, %{cluster: cluster} = state) do
    producer_opts = producer_opts(producer_opts, state.cluster_opts)

    {
      {cluster, topic_name, partition},
      {
        :poolboy,
        :start_link,
        [
          [
            name: {:via, Registry, {ProducerRegistry, {cluster, topic_name, partition}}},
            worker_module: PartitionedProducer,
            size: Keyword.get(producer_opts, :num_producers, @num_producers),
            max_overflow: 0,
            strategy: :fifo
          ],
          {topic_name, partition, producer_opts, state.cluster_opts}
        ]
      },
      :permanent,
      :infinity,
      :supervisor,
      [:poolboy, PartitionedProducer]
    }
  end

  defp start_producers(topic_name, producer_opts, %{cluster: cluster} = state) do
    with {:ok, {%Topic{partition: nil}, partitions}} <-
           PartitionManager.lookup(cluster, topic_name) do
      partitions = if partitions > 0, do: 0..(partitions - 1), else: [nil]

      partitions
      |> Enum.reduce_while(:ok, fn partition, :ok ->
        start_producer(topic_name, partition, producer_opts, state)
        |> case do
          {:ok, _} -> {:cont, :ok}
          err -> {:halt, err}
        end
      end)
    end
  end

  defp name(cluster), do: String.to_atom("#{__MODULE__}.#{cluster}")
end
