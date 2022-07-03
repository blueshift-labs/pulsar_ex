defmodule PulsarEx.ProducerSupervisor do
  use Supervisor

  alias PulsarEx.ProducerManager

  def start_link(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)
    Supervisor.start_link(__MODULE__, cluster_opts, name: name(cluster))
  end

  @impl true
  def init(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)

    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: ProducerManager.producers(cluster)},
      {ProducerManager, cluster_opts}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp name(cluster), do: String.to_atom("#{__MODULE__}.#{cluster}")
end
