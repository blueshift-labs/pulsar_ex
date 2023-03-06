defmodule PulsarEx.ProducerManager do
  use GenServer

  require Logger

  alias PulsarEx.{Cluster, Topic, ProducerRegistry, ProducerSupervisor, Producer}

  @num_producers 1

  def get_producer(cluster_name, %Topic{partition: partition} = topic, producer_opts \\ []) do
    topic_name = to_string(%{topic | partition: nil})

    with [] <- Registry.lookup(ProducerRegistry, {cluster_name, topic_name, partition}),
         {:ok, pool} <- GenServer.call(__MODULE__, {:create, cluster_name, topic, producer_opts}) do
      {:ok, :poolboy.transaction(pool, & &1)}
    else
      [{pool, _}] ->
        {:ok, :poolboy.transaction(pool, & &1)}

      err ->
        err
    end
  end

  def start_link(clusters) do
    GenServer.start_link(__MODULE__, clusters, name: __MODULE__)
  end

  @impl true
  def init(clusters) do
    Logger.debug("Starting producer manager")

    Process.flag(:trap_exit, true)

    {:ok, Enum.into(clusters, %{}, &{&1.cluster_name, &1})}
  end

  @impl true
  def handle_call(
        {:create, cluster_name, %Topic{partition: partition} = topic, producer_opts},
        _from,
        clusters
      ) do
    topic_name = to_string(%{topic | partition: nil})

    with cluster when not is_nil(cluster) <- Map.get(clusters, cluster_name),
         [] <- Registry.lookup(ProducerRegistry, {cluster_name, topic_name, partition}),
         {:ok, pool} <- start_producer(cluster, topic, producer_opts) do
      {:reply, {:ok, pool}, clusters}
    else
      nil ->
        {:reply, {:error, :cluster_not_configured}, clusters}

      [{pool, _}] ->
        {:reply, {:ok, pool}, clusters}

      err ->
        {:reply, err, clusters}
    end
  end

  @impl true
  def terminate(reason, state) do
    case reason do
      :shutdown ->
        Logger.debug("Shutting down Producer Manager, #{inspect(reason)}")

      :normal ->
        Logger.debug("Shutting down Producer Manager, #{inspect(reason)}")

      {:shutdown, _} ->
        Logger.debug("Shutting down Producer Manager, #{inspect(reason)}")

      _ ->
        Logger.error("Shutting down Producer Manager, #{inspect(reason)}")
    end

    state
  end

  defp start_producer(%Cluster{} = cluster, %Topic{} = topic, producer_opts) do
    started =
      DynamicSupervisor.start_child(
        ProducerSupervisor,
        producer_spec(cluster, topic, producer_opts)
      )

    case started do
      {:ok, _} ->
        Logger.debug("Started producer for topic [#{topic}], on cluster #{cluster}")

      {:error, err} ->
        Logger.error(
          "Error starting producer for topic [#{topic}], on cluster #{cluster}, #{inspect(err)}"
        )
    end

    started
  end

  defp producer_spec(
         %Cluster{cluster_name: cluster_name, cluster_opts: cluster_opts} = cluster,
         %Topic{partition: partition} = topic,
         producer_opts
       ) do
    topic_name = to_string(%{topic | partition: nil})

    producer_opts =
      cluster_opts
      |> Keyword.get(:producer_opts, [])
      |> Keyword.merge(producer_opts, fn
        :properties, v1, v2 -> Keyword.merge(v1, v2)
        _, _, v -> v
      end)

    {
      {cluster_name, topic_name, partition},
      {
        :poolboy,
        :start_link,
        [
          [
            name:
              {:via, Registry,
               {ProducerRegistry, {cluster_name, topic_name, partition}, producer_opts}},
            worker_module: Producer,
            size: Keyword.get(producer_opts, :num_producers, @num_producers),
            max_overflow: 0,
            strategy: :fifo
          ],
          {cluster, topic, producer_opts}
        ]
      },
      :permanent,
      :infinity,
      :supervisor,
      [:poolboy, Producer]
    }
  end
end
