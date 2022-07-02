defmodule PulsarEx.ClusterSupervisor do
  use Supervisor

  alias PulsarEx.{
    Executor,
    PartitionManager,
    ConnectionSupervisor,
    ProducerSupervisor,
    ConsumerSupervisor
  }

  def start_link(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)
    Supervisor.start_link(__MODULE__, cluster_opts, name: name(cluster))
  end

  @impl true
  def init(cluster_opts) do
    children = [
      Executor.pool_spec(cluster_opts),
      {PartitionManager, cluster_opts},
      {ConnectionSupervisor, cluster_opts},
      {ProducerSupervisor, cluster_opts},
      {ConsumerSupervisor, cluster_opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp name(cluster), do: String.to_atom("#{__MODULE__}.#{cluster}")
end
