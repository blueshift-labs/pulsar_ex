defmodule Mix.Tasks.PulsarEx.Setup do
  use Mix.Task

  alias PulsarEx.Cluster

  @shortdoc "Set up the pulsar cluster"
  def run(_) do
    Mix.Task.run("app.config")

    Application.get_all_env(:pulsar_ex)
    |> Keyword.drop([:shutdown_timeout, :producer_module])
    |> Keyword.pop(:clusters, [])
    |> case do
      {clusters, []} -> clusters
      {clusters, cluster} -> [cluster | clusters]
    end
    |> Enum.map(&Cluster.from_cluster_opts/1)
    |> Enum.each(&Cluster.setup!/1)
  end
end
