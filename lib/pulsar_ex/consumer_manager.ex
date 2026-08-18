defmodule PulsarEx.ConsumerManager do
  use GenServer

  require Logger

  alias PulsarEx.{Cluster, Topic, Admin, ConsumerSupervisor, ConsumerRegistry, PartitionManager}

  @num_consumers 1
  @refresh_interval 60_000
  @pattern_max_length 200

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

  def desired_consumers(cluster_name, tenant, namespace, subscription) do
    GenServer.call(
      __MODULE__,
      {:desired_consumers, cluster_name, tenant, namespace, subscription}
    )
  end

  def start_link(clusters) do
    GenServer.start_link(__MODULE__, clusters, name: __MODULE__)
  end

  @impl true
  def init(clusters) do
    Logger.debug("Starting consumer manager")

    Process.flag(:trap_exit, true)

    clusters = Enum.into(clusters, %{}, &{&1.cluster_name, &1})

    {:ok,
     %{
       clusters: clusters,
       timers: %{},
       discovery_snapshots: %{},
       exact_topic_snapshots: %{}
     }}
  end

  @impl true
  def handle_call(
        {:start, cluster_name, topic_name, subscription, module, consumer_opts},
        _from,
        %{
          clusters: clusters,
          timers: timers,
          exact_topic_snapshots: exact_topic_snapshots
        } = state
      ) do
    key = {cluster_name, topic_name, subscription}

    if Map.has_key?(timers, key) do
      {:reply, :ok, state}
    else
      with %Cluster{cluster_opts: cluster_opts} = cluster <- Map.get(clusters, cluster_name),
           {desired_partitions, nil} <-
             do_start_consumers(cluster, topic_name, subscription, module, consumer_opts) do
        refresh_interval =
          cluster_opts
          |> Keyword.get(:consumer_opts, [])
          |> Keyword.merge(consumer_opts)
          |> Keyword.get(:refresh_interval, @refresh_interval)

        refresh_ref = make_ref()

        timer_ref =
          Process.send_after(
            self(),
            {:refresh, cluster, topic_name, subscription, module, consumer_opts, refresh_interval,
             refresh_ref},
            refresh_interval
          )

        timers = Map.put(timers, key, {timer_ref, refresh_ref})

        exact_topic_snapshots =
          put_exact_topic_snapshot(
            exact_topic_snapshots,
            key,
            topic_name,
            desired_partitions,
            num_consumers(cluster_opts, consumer_opts)
          )

        {:reply, :ok, %{state | timers: timers, exact_topic_snapshots: exact_topic_snapshots}}
      else
        nil ->
          {:reply, {:error, :cluster_not_configured}, state}

        {:error, err} ->
          {:reply, {:error, err}, state}

        {_partitions, {:error, err}} ->
          {:reply, {:error, err}, state}
      end
    end
  end

  def handle_call(
        {:start, cluster_name, tenant, namespace, topic_name, subscription, module,
         consumer_opts},
        _from,
        %{clusters: clusters, timers: timers, discovery_snapshots: discovery_snapshots} = state
      ) do
    key = {cluster_name, tenant, namespace, topic_name, subscription}

    if Map.has_key?(timers, key) do
      {:reply, :ok, state}
    else
      start_time = System.monotonic_time()

      with %Cluster{cluster_opts: cluster_opts} = cluster <- Map.get(clusters, cluster_name),
           {:ok, topic_names, desired_partitions, failed_count, start_result} <-
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

        emit_discovery_complete(
          start_time,
          cluster_name,
          tenant,
          namespace,
          subscription,
          topic_name,
          :start,
          length(topic_names),
          desired_partitions,
          length(topic_names),
          0,
          failed_count
        )

        refresh_ref = make_ref()

        timer_ref =
          Process.send_after(
            self(),
            {:refresh, cluster, tenant, namespace, topic_name, subscription, module,
             consumer_opts, refresh_interval, refresh_ref},
            refresh_interval
          )

        timers = Map.put(timers, key, {timer_ref, refresh_ref, MapSet.new(topic_names)})

        discovery_snapshots =
          put_discovery_snapshot(
            discovery_snapshots,
            key,
            desired_partitions,
            num_consumers(cluster_opts, consumer_opts)
          )

        reply = start_result || :ok

        {:reply, reply, %{state | timers: timers, discovery_snapshots: discovery_snapshots}}
      else
        nil ->
          {:reply, {:error, :cluster_not_configured}, state}

        {:error, err} ->
          emit_discovery_error(
            start_time,
            cluster_name,
            tenant,
            namespace,
            subscription,
            topic_name,
            :start,
            err
          )

          {:reply, {:error, err}, state}
      end
    end
  end

  def handle_call(
        {:stop, cluster_name, topic_name, subscription},
        _from,
        %{
          clusters: clusters,
          timers: timers,
          exact_topic_snapshots: exact_topic_snapshots
        } = state
      ) do
    with %Cluster{} = cluster <- Map.get(clusters, cluster_name) do
      key = {cluster_name, topic_name, subscription}
      {entry, timers} = Map.pop(timers, key)
      {_snapshot, exact_topic_snapshots} = Map.pop(exact_topic_snapshots, key)

      case entry do
        {timer_ref, _refresh_ref} -> Process.cancel_timer(timer_ref)
        nil -> :ok
      end

      do_stop_consumers(cluster, topic_name, subscription)

      {:reply, :ok, %{state | timers: timers, exact_topic_snapshots: exact_topic_snapshots}}
    else
      nil ->
        {:reply, {:error, :cluster_not_configured}, state}
    end
  end

  def handle_call(
        {:stop, cluster_name, tenant, namespace, topic_name, subscription},
        _from,
        %{clusters: clusters, timers: timers, discovery_snapshots: discovery_snapshots} = state
      ) do
    with %Cluster{} = cluster <- Map.get(clusters, cluster_name) do
      key = {cluster_name, tenant, namespace, topic_name, subscription}
      {entry, timers} = Map.pop(timers, key)
      {_snapshot, discovery_snapshots} = Map.pop(discovery_snapshots, key)

      case entry do
        {timer_ref, _refresh_ref, matched_topics} ->
          Process.cancel_timer(timer_ref)
          Enum.each(matched_topics, &do_stop_consumers(cluster, &1, subscription))

        nil ->
          do_stop_matching_consumers(
            cluster_name,
            tenant,
            namespace,
            topic_name,
            subscription
          )
      end

      {:reply, :ok, %{state | timers: timers, discovery_snapshots: discovery_snapshots}}
    else
      nil ->
        {:reply, {:error, :cluster_not_configured}, state}
    end
  end

  def handle_call(
        {:desired_consumers, cluster_name, tenant, namespace, subscription},
        _from,
        %{
          discovery_snapshots: discovery_snapshots,
          exact_topic_snapshots: exact_topic_snapshots
        } = state
      ) do
    discovered_consumers =
      sum_discovery_desired_consumers(
        discovery_snapshots,
        cluster_name,
        tenant,
        namespace,
        subscription
      )

    exact_topic_consumers =
      exact_topic_snapshots
      |> Enum.filter(fn {{c, _topic_name, s}, snapshot} ->
        c == cluster_name and snapshot.tenant == tenant and snapshot.namespace == namespace and
          s == subscription
      end)
      |> Enum.map(fn {_key, snapshot} -> snapshot.desired_consumers end)
      |> Enum.sum()

    {:reply, discovered_consumers + exact_topic_consumers, state}
  end

  @impl true
  def handle_info(
        {:refresh, %Cluster{cluster_name: cluster_name, cluster_opts: cluster_opts} = cluster,
         topic_name, subscription, module, consumer_opts, refresh_interval, refresh_ref},
        %{timers: timers, exact_topic_snapshots: exact_topic_snapshots} = state
      ) do
    key = {cluster_name, topic_name, subscription}

    case Map.fetch(timers, key) do
      {:ok, {_timer_ref, ^refresh_ref}} ->
        exact_topic_snapshots =
          case do_start_consumers(
                 cluster,
                 topic_name,
                 subscription,
                 module,
                 consumer_opts
               ) do
            {desired_partitions, _start_result} when is_integer(desired_partitions) ->
              put_exact_topic_snapshot(
                exact_topic_snapshots,
                key,
                topic_name,
                desired_partitions,
                num_consumers(cluster_opts, consumer_opts)
              )

            {:error, _err} ->
              exact_topic_snapshots
          end

        new_refresh_ref = make_ref()

        timer_ref =
          Process.send_after(
            self(),
            {:refresh, cluster, topic_name, subscription, module, consumer_opts, refresh_interval,
             new_refresh_ref},
            refresh_interval
          )

        timers = Map.put(timers, key, {timer_ref, new_refresh_ref})

        {:noreply, %{state | timers: timers, exact_topic_snapshots: exact_topic_snapshots}}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(
        {:refresh, %Cluster{cluster_name: cluster_name, cluster_opts: cluster_opts} = cluster,
         tenant, namespace, topic_name, subscription, module, consumer_opts, refresh_interval,
         refresh_ref},
        %{timers: timers, discovery_snapshots: discovery_snapshots} = state
      ) do
    key = {cluster_name, tenant, namespace, topic_name, subscription}

    case Map.fetch(timers, key) do
      {:ok, {_timer_ref, ^refresh_ref, old_matched}} ->
        new_refresh_ref = make_ref()
        start_time = System.monotonic_time()

        {new_matched, discovery_snapshots} =
          case do_start_consumers(
                 cluster,
                 tenant,
                 namespace,
                 topic_name,
                 subscription,
                 module,
                 consumer_opts
               ) do
            {:ok, topic_names, desired_partitions, failed_count, _start_result} ->
              new_matched = MapSet.new(topic_names)
              removed_topics = MapSet.difference(old_matched, new_matched)

              Enum.each(removed_topics, &do_stop_consumers(cluster, &1, subscription))

              removed = MapSet.size(removed_topics)
              added = new_matched |> MapSet.difference(old_matched) |> MapSet.size()

              emit_discovery_complete(
                start_time,
                cluster_name,
                tenant,
                namespace,
                subscription,
                topic_name,
                :refresh,
                MapSet.size(new_matched),
                desired_partitions,
                added,
                removed,
                failed_count
              )

              discovery_snapshots =
                put_discovery_snapshot(
                  discovery_snapshots,
                  key,
                  desired_partitions,
                  num_consumers(cluster_opts, consumer_opts)
                )

              {new_matched, discovery_snapshots}

            {:error, err} ->
              emit_discovery_error(
                start_time,
                cluster_name,
                tenant,
                namespace,
                subscription,
                topic_name,
                :refresh,
                err
              )

              {old_matched, discovery_snapshots}
          end

        timer_ref =
          Process.send_after(
            self(),
            {:refresh, cluster, tenant, namespace, topic_name, subscription, module,
             consumer_opts, refresh_interval, new_refresh_ref},
            refresh_interval
          )

        timers = Map.put(timers, key, {timer_ref, new_refresh_ref, new_matched})

        {:noreply, %{state | timers: timers, discovery_snapshots: discovery_snapshots}}

      _ ->
        {:noreply, state}
    end
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
           admin_module().discover_topics(brokers, admin_port, tenant, namespace, topic_name) do
      {desired_partitions, failed_count, start_result} =
        Enum.reduce(topic_names, {0, 0, nil}, fn topic_name, {partitions_acc, failed_acc, err} ->
          case do_start_consumers(cluster, topic_name, subscription, module, consumer_opts) do
            {partitions, nil} ->
              {partitions_acc + partitions, failed_acc, err}

            {partitions, {:error, _} = new_err} ->
              {partitions_acc + partitions, failed_acc + 1, err || new_err}

            {:error, _} = new_err ->
              {partitions_acc, failed_acc + 1, err || new_err}
          end
        end)

      {:ok, topic_names, desired_partitions, failed_count, start_result}
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
           partition_manager_module().lookup(cluster_name, topic_name) do
      result =
        if partitions > 0 do
          Enum.reduce(0..(partitions - 1), nil, fn partition, err ->
            case do_start_consumer(
                   cluster,
                   %{topic | partition: partition},
                   subscription,
                   module,
                   consumer_opts
                 ) do
              nil -> err
              {:error, _} = new_err -> err || new_err
            end
          end)
        else
          do_start_consumer(cluster, topic, subscription, module, consumer_opts)
        end

      {max(partitions, 1), result}
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
           partition_manager_module().lookup(cluster_name, topic_name) do
      partitions = if partitions > 0, do: 0..(partitions - 1), else: [nil]

      partitions
      |> Enum.flat_map(fn partition ->
        Registry.lookup(
          ConsumerRegistry,
          {cluster_name, to_string(%{topic | partition: partition}), subscription}
        )
      end)
      |> Enum.each(fn {pid, _value} ->
        DynamicSupervisor.terminate_child(ConsumerSupervisor, pid)
      end)
    end
  end

  defp do_stop_matching_consumers(
         cluster_name,
         tenant,
         namespace,
         topic_name,
         subscription
       ) do
    ConsumerRegistry
    |> Registry.select([
      {{{cluster_name, :"$1", subscription}, :"$2", :_}, [], [{{:"$1", :"$2"}}]}
    ])
    |> Enum.each(fn {registered_topic_name, pid} ->
      with {:ok, %Topic{} = topic} <- Topic.parse(registered_topic_name),
           true <- selector_matches?(tenant, topic.tenant),
           true <- selector_matches?(namespace, topic.namespace),
           true <- selector_matches?(topic_name, topic.topic) do
        DynamicSupervisor.terminate_child(ConsumerSupervisor, pid)
      end
    end)
  end

  defp selector_matches?(%Regex{} = pattern, value), do: Regex.match?(pattern, value)
  defp selector_matches?(selector, value), do: selector == value

  defp admin_module() do
    Application.get_env(:pulsar_ex, :admin_module, Admin)
  end

  defp partition_manager_module() do
    Application.get_env(:pulsar_ex, :partition_manager_module, PartitionManager)
  end

  defp merge_consumer_opts(cluster_opts, consumer_opts) do
    cluster_opts
    |> Keyword.get(:consumer_opts, [])
    |> Keyword.merge(consumer_opts, fn
      :properties, v1, v2 -> Keyword.merge(v1, v2)
      _, _, v -> v
    end)
  end

  defp num_consumers(cluster_opts, consumer_opts) do
    merge_consumer_opts(cluster_opts, consumer_opts)
    |> Keyword.get(:num_consumers, @num_consumers)
  end

  defp bounded_pattern(topic_name) do
    inspect(topic_name) |> String.slice(0, @pattern_max_length)
  end

  defp discovery_metadata(cluster_name, tenant, namespace, subscription, topic_name, phase) do
    %{
      cluster: cluster_name,
      tenant: tenant,
      namespace: namespace,
      subscription: subscription,
      pattern: bounded_pattern(topic_name),
      phase: phase
    }
  end

  defp emit_discovery_complete(
         start_time,
         cluster_name,
         tenant,
         namespace,
         subscription,
         topic_name,
         phase,
         matched_topics,
         desired_partitions,
         added,
         removed,
         failed_topics
       ) do
    :telemetry.execute(
      [:pulsar_ex, :consumer_manager, :discovery, :complete],
      %{
        duration: System.monotonic_time() - start_time,
        matched_topics: matched_topics,
        desired_partitions: desired_partitions,
        added: added,
        removed: removed,
        failed_topics: failed_topics
      },
      discovery_metadata(cluster_name, tenant, namespace, subscription, topic_name, phase)
    )
  end

  defp emit_discovery_error(
         start_time,
         cluster_name,
         tenant,
         namespace,
         subscription,
         topic_name,
         phase,
         err
       ) do
    Logger.error(
      "Error discovering topics for pattern [#{inspect(topic_name)}], tenant [#{tenant}], namespace [#{namespace}], on cluster [#{cluster_name}], #{inspect(err)}"
    )

    :telemetry.execute(
      [:pulsar_ex, :consumer_manager, :discovery, :error],
      %{count: 1, duration: System.monotonic_time() - start_time},
      discovery_metadata(cluster_name, tenant, namespace, subscription, topic_name, phase)
    )
  end

  defp sum_discovery_desired_consumers(
         discovery_snapshots,
         cluster_name,
         tenant,
         namespace,
         subscription
       ) do
    discovery_snapshots
    |> Enum.filter(fn {{c, t, n, _topic_name, s}, _snapshot} ->
      c == cluster_name and t == tenant and n == namespace and s == subscription
    end)
    |> Enum.map(fn {_key, snapshot} -> snapshot.desired_consumers end)
    |> Enum.sum()
  end

  defp put_discovery_snapshot(discovery_snapshots, key, desired_partitions, num_consumers) do
    Map.put(discovery_snapshots, key, %{desired_consumers: desired_partitions * num_consumers})
  end

  defp put_exact_topic_snapshot(
         exact_topic_snapshots,
         key,
         topic_name,
         desired_partitions,
         num_consumers
       ) do
    {:ok, %Topic{tenant: tenant, namespace: namespace}} = Topic.parse(topic_name)

    Map.put(exact_topic_snapshots, key, %{
      tenant: tenant,
      namespace: namespace,
      desired_consumers: desired_partitions * num_consumers
    })
  end

  defp consumer_spec(
         %Cluster{cluster_name: cluster_name, cluster_opts: cluster_opts} = cluster,
         %Topic{} = topic,
         subscription,
         module,
         consumer_opts
       ) do
    consumer_opts = merge_consumer_opts(cluster_opts, consumer_opts)

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
