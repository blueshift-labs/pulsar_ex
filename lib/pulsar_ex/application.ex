defmodule PulsarEx.Application do
  use Application

  alias PulsarEx.{
    SignalHandler,
    Cluster,
    ConnectionRegistry,
    ProducerRegistry,
    ProducerIDRegistry,
    ConsumerRegistry,
    ConsumerIDRegistry,
    ConnectionSupervisor,
    ProducerSupervisor,
    ConsumerSupervisor,
    PartitionManager,
    ConnectionManager,
    ProducerManager,
    ConsumerManager
  }

  @globals :pulsar_globals

  def shutdown!() do
    :ets.insert(@globals, {:shutdown, true})
  end

  def shutdown?() do
    :ets.lookup(@globals, :shutdown) == [shutdown: true]
  end

  def get_producer_id(cluster_name) do
    :ets.update_counter(
      @globals,
      {:producer_id, cluster_name},
      {2, 1},
      {{:producer_id, cluster_name}, 0}
    )
  end

  def get_consumer_id(cluster_name) do
    :ets.update_counter(
      @globals,
      {:consumer_id, cluster_name},
      {2, 1},
      {{:consumer_id, cluster_name}, 0}
    )
  end

  @impl true
  def start(_type, _args) do
    children = setup!() |> children()

    opts = [strategy: :rest_for_one, name: PulsarEx.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def setup!() do
    unless Enum.member?(:ets.all(), @globals) do
      :ets.new(@globals, [
        :named_table,
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])
    end

    SignalHandler.start_link()

    clusters =
      Application.get_all_env(:pulsar_ex)
      |> Keyword.drop([:shutdown_timeout, :producer_module])
      |> Keyword.pop(:clusters, [])
      |> case do
        {clusters, []} -> clusters
        {clusters, cluster} -> [cluster | clusters]
      end
      |> Enum.map(&Cluster.from_cluster_opts/1)

    clusters |> Enum.each(&PulsarEx.Cluster.setup!/1)

    PartitionManager.setup()

    clusters
  end

  def children(clusters) do
    [
      {Registry, keys: :unique, name: ConnectionRegistry},
      {Registry, keys: :unique, name: ProducerRegistry},
      {Registry, keys: :unique, name: ProducerIDRegistry},
      {Registry, keys: :unique, name: ConsumerRegistry},
      {Registry, keys: :unique, name: ConsumerIDRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: ConnectionSupervisor},
      {DynamicSupervisor, strategy: :one_for_one, name: ProducerSupervisor},
      {DynamicSupervisor, strategy: :one_for_one, name: ConsumerSupervisor},
      {PartitionManager, clusters},
      {ConnectionManager, clusters},
      {ProducerManager, clusters},
      {ConsumerManager, clusters}
    ]
  end

  def destroy!() do
    if Enum.member?(:ets.all(), @globals) do
      :ets.delete(@globals)
    end
  end
end
