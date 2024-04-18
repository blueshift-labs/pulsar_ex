defmodule PulsarEx.ConsumerManager do
  use GenServer

  require Logger

  alias PulsarEx.{Cluster, Topic, Admin, ConsumerSupervisor, ConsumerRegistry, PartitionManager}

  @num_consumers 1
  @refresh_interval 60_000

  def start_consumer(cluster_name, topic_name, subscription, module, consumer_opts) do
    GenServer.call(
      __MODULE__,
      {:start, cluster_name, topic_name, subscription, module, consumer_opts}
    )
  end

  def start_consumer(
        cluster_name,
        tenant,
        namespace,
        topic_name,
        subscription,
        module,
        consumer_opts
      ) do
    GenServer.call(
      __MODULE__,
      {:start, cluster_name, tenant, namespace, topic_name, subscription, module, consumer_opts}
    )
  end

  def stop_consumer(cluster_name, topic_name, subscription) do
    GenServer.call(__MODULE__, {:stop, cluster_name, topic_name, subscription})
  end

  def stop_consumer(cluster_name, tenant, namespace, topic_name, subscription) do
    GenServer.call(__MODULE__, {:stop, cluster_name, tenant, namespace, topic_name, subscription})
  end

  def start_link(clusters) do
    GenServer.start_link(__MODULE__, clusters, name: __MODULE__)
  end

  @impl true
  def init(clusters) do
    Logger.debug("Starting consumer manager")

    Process.flag(:trap_exit, true)

    clusters = Enum.into(clusters, %{}, &{&1.cluster_name, &1})
    {:ok, %{clusters: clusters, timers: %{}}}
  end

  @impl true
  def handle_call(
        {:start, cluster_name, topic_name, subscription, module, consumer_opts},
        _from,
        %{clusters: clusters, timers: timers} = state
      ) do
    with %Cluster{cluster_opts: cluster_opts} = cluster <- Map.get(clusters, cluster_name),
         nil <- do_start_consumers(cluster, topic_name, subscription, module, consumer_opts) do
      refresh_interval =
        cluster_opts
        |> Keyword.get(:consumer_opts, [])
        |> Keyword.merge(consumer_opts)
        |> Keyword.get(:refresh_interval, @refresh_interval)

      ref =
        Process.send_after(
          self(),
          {:refresh, cluster, topic_name, subscription, module, consumer_opts, refresh_interval},
          refresh_interval
        )

      timers = Map.put(timers, {cluster_name, topic_name, subscription}, ref)
      {:reply, :ok, %{clusters: clusters, timers: timers}}
    else
      nil ->
        {:reply, {:error, :cluster_not_configured}, state}

      {:error, err} ->
        {:reply, {:error, err}, state}
    end
  end

  def handle_call(
        {:start, cluster_name, tenant, namespace, topic_name, subscription, module,
         consumer_opts},
        _from,
        %{clusters: clusters, timers: timers} = state
      ) do
    with %Cluster{cluster_opts: cluster_opts} = cluster <- Map.get(clusters, cluster_name),
         nil <-
           do_start_consumers(
             cluster,
             tenant,
             namespace,
             topic_name,
             subscription,
             module,
             consumer_opts
           ) do
      refresh_interval =
        cluster_opts
        |> Keyword.get(:consumer_opts, [])
        |> Keyword.merge(consumer_opts)
        |> Keyword.get(:refresh_interval, @refresh_interval)

      ref =
        Process.send_after(
          self(),
          {:refresh, cluster, tenant, namespace, topic_name, subscription, module, consumer_opts,
           refresh_interval},
          refresh_interval
        )

      timers = Map.put(timers, {cluster_name, tenant, namespace, topic_name, subscription}, ref)
      {:reply, :ok, %{clusters: clusters, timers: timers}}
    else
      nil ->
        {:reply, {:error, :cluster_not_configured}, state}

      {:error, err} ->
        {:reply, {:error, err}, state}
    end
  end

  def handle_call(
        {:stop, cluster_name, topic_name, subscription},
        _from,
        %{clusters: clusters, timers: timers} = state
      ) do
    with %Cluster{} = cluster <- Map.get(clusters, cluster_name) do
      {ref, timers} = Map.pop(timers, {cluster_name, topic_name, subscription})

      if ref do
        Process.cancel_timer(ref)
      end

      do_stop_consumers(cluster, topic_name, subscription)

      {:reply, :ok, %{state | timers: timers}}
    else
      nil ->
        {:reply, {:error, :cluster_not_configured}, state}
    end
  end

  def handle_call(
        {:stop, cluster_name, tenant, namespace, topic_name, subscription},
        _from,
        %{clusters: clusters, timers: timers} = state
      ) do
    with %Cluster{} = cluster <- Map.get(clusters, cluster_name) do
      {ref, timers} = Map.pop(timers, {cluster_name, tenant, namespace, topic_name, subscription})

      if ref do
        Process.cancel_timer(ref)
      end

      do_stop_consumers(cluster, tenant, namespace, topic_name, subscription)

      {:reply, :ok, %{state | timers: timers}}
    else
      nil ->
        {:reply, {:error, :cluster_not_configured}, state}
    end
  end

  @impl true
  def handle_info(
        {:refresh, %Cluster{cluster_name: cluster_name} = cluster, topic_name, subscription,
         module, consumer_opts, refresh_interval},
        %{timers: timers} = state
      ) do
    do_start_consumers(cluster, topic_name, subscription, module, consumer_opts)

    ref =
      Process.send_after(
        self(),
        {:refresh, cluster, topic_name, subscription, module, consumer_opts, refresh_interval},
        refresh_interval
      )

    timers = Map.put(timers, {cluster_name, topic_name, subscription}, ref)
    {:noreply, %{state | timers: timers}}
  end

  def handle_info(
        {:refresh, %Cluster{cluster_name: cluster_name} = cluster, tenant, namespace, topic_name,
         subscription, module, consumer_opts, refresh_interval},
        %{timers: timers} = state
      ) do
    do_start_consumers(
      cluster,
      tenant,
      namespace,
      topic_name,
      subscription,
      module,
      consumer_opts
    )

    ref =
      Process.send_after(
        self(),
        {:refresh, cluster, tenant, namespace, topic_name, subscription, module, consumer_opts,
         refresh_interval},
        refresh_interval
      )

    timers = Map.put(timers, {cluster_name, tenant, namespace, topic_name, subscription}, ref)
    {:noreply, %{state | timers: timers}}
  end

  @impl true
  def terminate(reason, state) do
    case reason do
      :shutdown ->
        Logger.debug("Shutting down Consumer Manager, #{inspect(reason)}")

      :normal ->
        Logger.debug("Shutting down Consumer Manager, #{inspect(reason)}")

      {:shutdown, _} ->
        Logger.debug("Shutting down Consumer Manager, #{inspect(reason)}")

      _ ->
        Logger.error("Shutting down Consumer Manager, #{inspect(reason)}")
    end

    state
  end

  defp do_start_consumers(
         %Cluster{brokers: brokers, admin_port: admin_port} = cluster,
         tenant,
         namespace,
         topic_name,
         subscription,
         module,
         consumer_opts
       ) do
    with {:ok, topic_names} <-
           Admin.discover_topics(brokers, admin_port, tenant, namespace, topic_name) do
      Enum.reduce(topic_names, nil, fn topic_name, err ->
        err || do_start_consumers(cluster, topic_name, subscription, module, consumer_opts)
      end)
    end
  end

  defp do_start_consumers(
         %Cluster{cluster_name: cluster_name} = cluster,
         topic_name,
         subscription,
         module,
         consumer_opts
       ) do
    with {:ok, {%Topic{partition: nil} = topic, partitions}} <-
           PartitionManager.lookup(cluster_name, topic_name) do
      if partitions > 0 do
        Enum.reduce(0..(partitions - 1), nil, fn partition, err ->
          err ||
            do_start_consumer(
              cluster,
              %{topic | partition: partition},
              subscription,
              module,
              consumer_opts
            )
        end)
      else
        do_start_consumer(cluster, topic, subscription, module, consumer_opts)
      end
    end
  end

  defp do_start_consumer(
         %Cluster{} = cluster,
         %Topic{} = topic,
         subscription,
         module,
         consumer_opts
       ) do
    DynamicSupervisor.start_child(
      ConsumerSupervisor,
      consumer_spec(cluster, topic, subscription, module, consumer_opts)
    )
    |> case do
      {:ok, _} ->
        nil

      {:error, {:already_started, _}} ->
        nil

      {:error, err} ->
        {:error, err}
    end
  end

  defp do_stop_consumers(%Cluster{cluster_name: cluster_name}, topic_name, subscription) do
    with {:ok, {%Topic{partition: nil} = topic, partitions}} <-
           PartitionManager.lookup(cluster_name, topic_name) do
      partitions = if partitions > 0, do: 0..(partitions - 1), else: [nil]

      partitions
      |> Enum.flat_map(fn partition ->
        Registry.lookup(
          ConsumerRegistry,
          {cluster_name, to_string(%{topic | partition: partition}), subscription}
        )
      end)
      |> Enum.each(&DynamicSupervisor.terminate_child(ConsumerSupervisor, &1))
    end
  end

  defp do_stop_consumers(
         %Cluster{brokers: brokers, admin_port: admin_port} = cluster,
         tenant,
         namespace,
         topic_name,
         subscription
       ) do
    with {:ok, topic_names} <-
           Admin.discover_topics(brokers, admin_port, tenant, namespace, topic_name) do
      Enum.each(topic_names, &do_stop_consumers(cluster, &1, subscription))
    end
  end

  defp consumer_spec(
         %Cluster{cluster_name: cluster_name, cluster_opts: cluster_opts} = cluster,
         %Topic{} = topic,
         subscription,
         module,
         consumer_opts
       ) do
    consumer_opts =
      cluster_opts
      |> Keyword.get(:consumer_opts, [])
      |> Keyword.merge(consumer_opts, fn
        :properties, v1, v2 -> Keyword.merge(v1, v2)
        _, _, v -> v
      end)

    consumers = Keyword.get(consumer_opts, :num_consumers, @num_consumers)

    children =
      for n <- 0..(consumers - 1) do
        Supervisor.child_spec({module, {cluster, topic, subscription, consumer_opts}},
          id: {cluster_name, to_string(topic), subscription, n}
        )
      end

    %{
      id: {cluster_name, to_string(topic), subscription},
      start: {
        Supervisor,
        :start_link,
        [
          children,
          [
            strategy: :one_for_one,
            name:
              {:via, Registry, {ConsumerRegistry, {cluster_name, to_string(topic), subscription}}}
          ]
        ]
      },
      restart: :permanent,
      shutdown: :infinity,
      type: :supervisor,
      modules: [module]
    }
  end
end
