defmodule PulsarEx.Cluster do
  require Logger

  def setup!(cluster_opts) do
    auto_setup = Keyword.get(cluster_opts, :auto_setup, false)

    cluster = Keyword.fetch!(cluster_opts, :cluster)
    hosts = Keyword.fetch!(cluster_opts, :brokers)
    port = Keyword.fetch!(cluster_opts, :admin_port)

    tenants = Keyword.get(cluster_opts, :tenants, [])
    namespaces = Keyword.get(cluster_opts, :namespaces, [])
    topics = Keyword.get(cluster_opts, :topics, [])
    partitioned_topics = Keyword.get(cluster_opts, :partitioned_topics, [])

    if auto_setup do
      Application.load(:pulsar_ex)
      Application.ensure_all_started(:hackney)

      Logger.info("Setting up pulsar cluster #{cluster}")

      {:ok, clusters} = PulsarEx.Admin.discover_clusters(hosts, port)

      tenants
      |> Enum.each(fn tenant ->
        :ok = PulsarEx.Admin.create_tenant(hosts, port, tenant, clusters)
        Logger.info("Created tenant #{tenant} on cluster #{cluster}")
      end)

      namespaces
      |> Enum.each(fn namespace ->
        :ok = PulsarEx.Admin.create_namespace(hosts, port, namespace)
        Logger.info("Created namespace #{namespace} on cluster #{cluster}")
      end)

      topics
      |> Enum.each(fn topic ->
        :ok = PulsarEx.Admin.create_topic(hosts, port, topic)
        Logger.info("Created topic #{topic} on cluster #{cluster}")
      end)

      partitioned_topics
      |> Enum.each(fn partitioned_topic ->
        :ok = PulsarEx.Admin.create_partitioned_topic(hosts, port, partitioned_topic)
        Logger.info("Created partitioned topic #{partitioned_topic} on cluster #{cluster}")
      end)
    end
  end
end
