defmodule PulsarEx.Cluster do
  defstruct cluster_name: "default",
            brokers: ["localhost"],
            port: 6650,
            admin_port: 8080,
            auto_connect: false,
            cluster_opts: []

  def from_cluster_opts(cluster_opts) do
    cluster_name = Keyword.get(cluster_opts, :cluster_name, "default")

    {opts, cluster_opts} =
      Keyword.split(cluster_opts, [:cluster_name, :brokers, :port, :admin_port])

    cluster = struct(__MODULE__, Enum.into(opts, %{}))
    %{cluster | cluster_name: "#{cluster_name}", cluster_opts: cluster_opts}
  end

  alias __MODULE__
  alias PulsarEx.{ProducerImpl, ConsumerManager}

  require Logger

  def setup!(%Cluster{
        cluster_name: cluster_name,
        brokers: brokers,
        admin_port: admin_port,
        cluster_opts: cluster_opts
      }) do
    auto_setup = Keyword.get(cluster_opts, :auto_setup, false)

    tenants = Keyword.get(cluster_opts, :tenants, [])
    namespaces = Keyword.get(cluster_opts, :namespaces, [])
    topics = Keyword.get(cluster_opts, :topics, [])

    if auto_setup do
      Application.load(:pulsar_ex)
      Application.ensure_all_started(:hackney)

      Logger.info("Setting up pulsar cluster [#{cluster_name}]")

      {:ok, clusters} = PulsarEx.Admin.discover_clusters(brokers, admin_port)

      tenants
      |> Enum.each(fn tenant ->
        :ok = PulsarEx.Admin.create_tenant(brokers, admin_port, tenant, clusters)
        Logger.info("Created tenant [#{tenant}] on cluster [#{cluster_name}]")
      end)

      namespaces
      |> Enum.each(fn namespace ->
        :ok = PulsarEx.Admin.create_namespace(brokers, admin_port, namespace)
        Logger.info("Created namespace [#{namespace}] on cluster [#{cluster_name}]")
      end)

      topics
      |> Enum.each(fn
        {partitioned_topic, partitions} ->
          :ok =
            PulsarEx.Admin.create_partitioned_topic(
              brokers,
              admin_port,
              partitioned_topic,
              partitions
            )

          Logger.info(
            "Created partitioned topic [#{partitioned_topic}]/[#{partitions}] on cluster [#{cluster_name}]"
          )

        topic ->
          :ok = PulsarEx.Admin.create_topic(brokers, admin_port, topic)
          Logger.info("Created topic [#{topic}] on cluster [#{cluster_name}]")
      end)
    end
  end

  def produce(cluster_name, topic_name, payload, message_opts \\ [], producer_opts \\ [])

  def produce(cluster_name, topic_name, payload, message_opts, %{} = producer_opts) do
    produce(cluster_name, topic_name, payload, message_opts, Enum.into(producer_opts, []))
  end

  def produce(cluster_name, topic_name, payload, message_opts, producer_opts) do
    producer_module = Application.get_env(:pulsar_ex, :producer_module, ProducerImpl)
    producer_module.produce(cluster_name, topic_name, payload, message_opts, producer_opts)
  end

  def start_consumer(cluster_name, tenant, namespace, topic_name, subscription, module, opts) do
    ConsumerManager.start_consumer(
      cluster_name,
      tenant,
      namespace,
      topic_name,
      subscription,
      module,
      opts
    )
  end

  def start_consumer(cluster_name, topic_name, subscription, module, opts) do
    ConsumerManager.start_consumer(cluster_name, topic_name, subscription, module, opts)
  end

  def stop_consumer(cluster_name, tenant, namespace, topic_name, subscription) do
    ConsumerManager.stop_consumer(cluster_name, tenant, namespace, topic_name, subscription)
  end

  def stop_consumer(cluster_name, topic_name, subscription) do
    ConsumerManager.stop_consumer(cluster_name, topic_name, subscription)
  end
end

defimpl String.Chars, for: PulsarEx.Cluster do
  def to_string(%PulsarEx.Cluster{cluster_name: cluster_name}) do
    cluster_name
  end
end
