defmodule PulsarEx.Application do
  @moduledoc false

  use Application

  require Logger

  alias PulsarEx.{
    SignalHandler,
    ClusterSupervisor,
    ConnectionRegistry,
    ProducerRegistry,
    ConsumerRegistry
  }

  @tab :pulsar_globals
  @partitions_table :pulsar_partitions

  def partitions_table(), do: @partitions_table

  def shutdown!() do
    :ets.insert(@tab, {:shutdown, true})
  end

  def shutdown?() do
    :ets.lookup(@tab, :shutdown) == [shutdown: true]
  end

  @impl true
  def start(_type, _args) do
    :ets.new(@tab, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    SignalHandler.start_link()

    :ets.new(@partitions_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    children = [
      {Registry, keys: :unique, name: ConnectionRegistry, partitions: System.schedulers_online()},
      {Registry, keys: :unique, name: ProducerRegistry, partitions: System.schedulers_online()},
      {Registry, keys: :unique, name: ConsumerRegistry, partitions: System.schedulers_online()}
    ]

    clusters =
      Application.get_all_env(:pulsar_ex)
      |> Keyword.drop([:shutdown_timeout, :producer_module])
      |> Keyword.pop(:clusters, [])
      |> case do
        {clusters, []} -> clusters
        {clusters, cluster} -> [cluster | clusters]
      end
      |> Enum.map(fn cluster_opts ->
        cluster = Keyword.get(cluster_opts, :cluster, :default)
        cluster = String.to_atom("#{cluster}")
        cluster_opts = Keyword.put(cluster_opts, :cluster, cluster)
        Supervisor.child_spec({ClusterSupervisor, cluster_opts}, id: {ClusterSupervisor, cluster})
      end)

    children = children ++ clusters

    Logger.info(inspect(Application.get_all_env(:pulsar_ex)))

    Logger.info(inspect(children))
    opts = [strategy: :one_for_one, name: PulsarEx.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
