defmodule Mix.Tasks.PulsarEx.Setup do
  use Mix.Task

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
    |> Enum.map(fn cluster_opts ->
      cluster = Keyword.get(cluster_opts, :cluster, :default)
      cluster = String.to_atom("#{cluster}")

      cluster_opts
      |> Keyword.put(:cluster, cluster)
      |> Keyword.put(:auto_setup, true)
      |> PulsarEx.Cluster.setup!()
    end)
  end
end
