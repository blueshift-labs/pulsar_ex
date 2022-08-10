defmodule PulsarEx.ConsumerManager do
  defmodule State do
    @enforce_keys [
      :cluster,
      :cluster_opts,
      :brokers,
      :admin_port,
      :refs
    ]

    defstruct [
      :cluster,
      :cluster_opts,
      :brokers,
      :admin_port,
      :refs
    ]
  end

  use GenServer

  require Logger

  alias PulsarEx.{Consumer, ConsumerRegistry, PartitionManager, Topic, Admin}

  @num_consumers 1
  @refresh_interval 60_000
  @connection_timeout 60_000

  def consumers(cluster), do: String.to_atom("PulsarEx.Consumers.#{cluster}")

  def start_consumer(
        cluster,
        tenant,
        namespace,
        topic,
        subscription,
        module,
        consumer_opts
      ) do
    GenServer.call(
      name(cluster),
      {:start, tenant, namespace, topic, subscription, module, consumer_opts},
      @connection_timeout
    )
  end

  def start_consumer(cluster, topic_name, subscription, module, consumer_opts) do
    GenServer.call(
      name(cluster),
      {:start, topic_name, nil, subscription, module, consumer_opts},
      @connection_timeout
    )
  end

  def start_consumer(cluster, topic_name, partitions, subscription, module, consumer_opts) do
    GenServer.call(
      name(cluster),
      {:start, topic_name, partitions, subscription, module, consumer_opts},
      @connection_timeout
    )
  end

  def stop_consumer(cluster, tenant, namespace, topic, subscription) do
    GenServer.call(name(cluster), {:stop, tenant, namespace, topic, subscription})
  end

  def stop_consumer(cluster, topic_name, subscription) do
    GenServer.call(name(cluster), {:stop, topic_name, nil, subscription})
  end

  def stop_consumer(cluster, topic_name, partitions, subscription) do
    GenServer.call(name(cluster), {:stop, topic_name, partitions, subscription})
  end

  def idle_time(cluster, topic_name, subscription) do
    with {:ok, {%Topic{}, partitions}} <- PartitionManager.lookup(cluster, topic_name) do
      partitions = if partitions > 0, do: 0..(partitions - 1), else: [nil]

      partitions
      |> Enum.flat_map(fn partition ->
        Registry.lookup(ConsumerRegistry, {cluster, topic_name, partition, subscription})
      end)
      |> Enum.flat_map(fn {sup, _} ->
        Supervisor.which_children(sup)
      end)
      |> Enum.map(fn {_, consumer, _, _} ->
        Consumer.idle_time(consumer)
      end)
      |> Enum.min(fn -> 0 end)
    end
  end

  def start_link(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)
    GenServer.start_link(__MODULE__, cluster_opts, name: name(cluster))
  end

  @impl true
  def init(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)

    Logger.debug("Starting consumer manager, on cluster #{cluster}")

    brokers = Keyword.fetch!(cluster_opts, :brokers)
    admin_port = Keyword.fetch!(cluster_opts, :admin_port)

    Process.flag(:trap_exit, true)

    auto_start =
      cluster_opts
      |> Keyword.get(:consumer_opts, [])
      |> Keyword.get(:auto_start, false)

    if auto_start do
      Keyword.get(cluster_opts, :consumers, [])
      |> Enum.each(fn consumer_opts ->
        subscription = Keyword.fetch!(consumer_opts, :subscription)
        module = Keyword.fetch!(consumer_opts, :module)

        case Keyword.get(consumer_opts, :tenant) do
          nil ->
            topic_name = Keyword.fetch!(consumer_opts, :topic)
            partitions = Keyword.get(consumer_opts, :partitions, nil)

            Process.send(
              self(),
              {:start, topic_name, partitions, subscription, module, consumer_opts},
              []
            )

          tenant ->
            namespace = Keyword.fetch!(consumer_opts, :namespace)
            topic = Keyword.fetch!(consumer_opts, :topic)

            Process.send(
              self(),
              {:start, tenant, namespace, topic, subscription, module, consumer_opts},
              []
            )
        end
      end)
    end

    {:ok,
     %State{
       cluster: cluster,
       cluster_opts: cluster_opts,
       brokers: brokers,
       admin_port: admin_port,
       refs: %{}
     }}
  end

  @impl true
  def handle_call(
        {:start, tenant, namespace, topic, subscription, module, consumer_opts},
        _from,
        state
      ) do
    consumer_opts = consumer_opts(consumer_opts, state.cluster_opts)

    case Admin.discover_topics(state.brokers, state.admin_port, tenant, namespace, topic) do
      {:ok, topic_names} ->
        auto_refresh = Keyword.get(consumer_opts, :auto_refresh, true)
        refresh_interval = Keyword.get(consumer_opts, :refresh_interval, @refresh_interval)

        case do_start_consumers(topic_names, subscription, module, consumer_opts, state) do
          nil ->
            if auto_refresh do
              ref =
                Process.send_after(
                  self(),
                  {:refresh, tenant, namespace, topic, subscription, module, consumer_opts},
                  refresh_interval + :rand.uniform(refresh_interval)
                )

              {:reply, :ok,
               %State{
                 state
                 | refs: Map.put(state.refs, {tenant, namespace, topic, subscription}, ref)
               }}
            else
              {:reply, :ok, state}
            end

          {:error, _} = err ->
            {:reply, err, state}
        end

      {:error, err} ->
        Logger.error(
          "Error discovering topics for tenant: #{inspect(tenant)}, namespace: #{inspect(namespace)}, topic: #{inspect(topic)}, on cluster #{state.cluster}, #{inspect(err)}"
        )

        {:reply, {:error, err}, state}
    end
  end

  @impl true
  def handle_call(
        {:start, topic_name, nil, subscription, module, consumer_opts},
        _from,
        state
      ) do
    consumer_opts = consumer_opts(consumer_opts, state.cluster_opts)

    auto_refresh = Keyword.get(consumer_opts, :auto_refresh, true)
    refresh_interval = Keyword.get(consumer_opts, :refresh_interval, @refresh_interval)

    case do_start_consumers(
           topic_name,
           nil,
           subscription,
           module,
           consumer_opts,
           state
         ) do
      nil ->
        if auto_refresh do
          ref =
            Process.send_after(
              self(),
              {:refresh, topic_name, subscription, module, consumer_opts},
              refresh_interval + :rand.uniform(refresh_interval)
            )

          {:reply, :ok,
           %State{state | refs: Map.put(state.refs, {topic_name, subscription}, ref)}}
        else
          {:reply, :ok, state}
        end

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  @impl true
  def handle_call(
        {:start, topic_name, partitions, subscription, module, consumer_opts},
        _from,
        state
      ) do
    consumer_opts = consumer_opts(consumer_opts, state.cluster_opts)

    case do_start_consumers(
           topic_name,
           partitions,
           subscription,
           module,
           consumer_opts,
           state
         ) do
      nil ->
        {:reply, :ok, state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  @impl true
  def handle_call(
        {:stop, tenant, namespace, topic, subscription},
        _from,
        state
      ) do
    {ref, refs} = Map.pop(state.refs, {tenant, namespace, topic, subscription})

    if ref != nil do
      Process.cancel_timer(ref)
    end

    state = %State{state | refs: refs}

    case Admin.discover_topics(state.brokers, state.admin_port, tenant, namespace, topic) do
      {:ok, topic_names} ->
        do_stop_consumers(topic_names, subscription, state)
        {:reply, :ok, state}

      {:error, err} ->
        Logger.error(
          "Error discovering topics for tenant: #{inspect(tenant)}, namespace: #{inspect(namespace)}, topic: #{inspect(topic)}, on cluster #{state.cluster}, #{inspect(err)}"
        )

        {:reply, {:error, err}, state}
    end
  end

  @impl true
  def handle_call({:stop, topic_name, nil, subscription}, _from, state) do
    {ref, refs} = Map.pop(state.refs, {topic_name, subscription})

    if ref != nil do
      Process.cancel_timer(ref)
    end

    do_stop_consumers(topic_name, nil, subscription, state)
    {:reply, :ok, %State{state | refs: refs}}
  end

  @impl true
  def handle_call({:stop, topic_name, partitions, subscription}, _from, state) do
    do_stop_consumers(topic_name, partitions, subscription, state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:refresh, topic_name, subscription, module, consumer_opts}, state) do
    refresh_interval = Keyword.get(consumer_opts, :refresh_interval, @refresh_interval)
    do_start_consumers(topic_name, nil, subscription, module, consumer_opts, state)

    ref =
      Process.send_after(
        self(),
        {:refresh, topic_name, subscription, module, consumer_opts},
        refresh_interval + :rand.uniform(refresh_interval)
      )

    {:noreply, %State{state | refs: Map.put(state.refs, {topic_name, subscription}, ref)}}
  end

  @impl true
  def handle_info(
        {:refresh, tenant, namespace, topic, subscription, module, consumer_opts},
        state
      ) do
    refresh_interval = Keyword.get(consumer_opts, :refresh_interval, @refresh_interval)

    case Admin.discover_topics(state.brokers, state.admin_port, tenant, namespace, topic) do
      {:ok, topic_names} ->
        do_start_consumers(topic_names, subscription, module, consumer_opts, state)

      {:error, err} ->
        Logger.error(
          "Error discovering topics for tenant: #{inspect(tenant)}, namespace: #{inspect(namespace)}, topic: #{inspect(topic)}, on cluster #{state.cluster}, #{inspect(err)}"
        )
    end

    ref =
      Process.send_after(
        self(),
        {:refresh, tenant, namespace, topic, subscription, module, consumer_opts},
        refresh_interval + :rand.uniform(refresh_interval)
      )

    {:noreply,
     %State{state | refs: Map.put(state.refs, {tenant, namespace, topic, subscription}, ref)}}
  end

  @impl true
  def terminate(reason, state) do
    case reason do
      :shutdown ->
        Logger.info(
          "Shutting down Consumer Manager, on cluster #{state.cluster}, #{inspect(reason)}"
        )

      :normal ->
        Logger.info(
          "Shutting down Consumer Manager, on cluster #{state.cluster}, #{inspect(reason)}"
        )

      {:shutdown, _} ->
        Logger.info(
          "Shutting down Consumer Manager, on cluster #{state.cluster}, #{inspect(reason)}"
        )

      _ ->
        Logger.error(
          "Shutting down Consumer Manager, on cluster #{state.cluster}, #{inspect(reason)}"
        )
    end

    state
  end

  defp consumer_opts(consumer_opts, cluster_opts) do
    Keyword.merge(
      Keyword.get(cluster_opts, :consumer_opts, []),
      consumer_opts,
      fn
        :properties, v1, v2 -> Keyword.merge(v1, v2)
        _, _, v -> v
      end
    )
  end

  defp consumer_spec(
         topic_name,
         partition,
         subscription,
         module,
         consumer_opts,
         %{cluster: cluster} = state
       ) do
    consumers = Keyword.get(consumer_opts, :num_consumers, @num_consumers)

    children =
      for n <- 0..(consumers - 1) do
        Supervisor.child_spec(
          {module, {topic_name, partition, subscription, consumer_opts, state.cluster_opts}},
          id: {module, n}
        )
      end

    %{
      id: {cluster, topic_name, partition, subscription},
      start: {
        Supervisor,
        :start_link,
        [
          children,
          [
            strategy: :one_for_one,
            name:
              {:via, Registry, {ConsumerRegistry, {cluster, topic_name, partition, subscription}}}
          ]
        ]
      },
      restart: :permanent,
      shutdown: :infinity,
      type: :supervisor,
      modules: [module]
    }
  end

  defp do_start_consumer(
         topic_name,
         partition,
         subscription,
         module,
         consumer_opts,
         %{cluster: cluster} = state
       ) do
    Logger.debug(
      "Starting consumer for topic #{topic_name}, partition #{partition} with subscription #{subscription}, on cluster #{cluster}"
    )

    DynamicSupervisor.start_child(
      consumers(cluster),
      consumer_spec(topic_name, partition, subscription, module, consumer_opts, state)
    )
    |> case do
      {:ok, _} ->
        Logger.debug(
          "Started consumer for topic #{topic_name}, partition #{partition} with subscription #{subscription}, on cluster #{cluster}"
        )

        nil

      {:error, {:already_started, _}} ->
        nil

      {:error, err} ->
        Logger.error(
          "Error starting consumer for topic #{topic_name}, partition #{partition} with subscription #{subscription}, on cluster #{cluster}, #{inspect(err)}"
        )

        {:error, err}
    end
  end

  defp do_start_consumers(topic_names, subscription, module, consumer_opts, state) do
    topic_names
    |> Enum.reduce(nil, fn topic_name, err ->
      err ||
        do_start_consumers(topic_name, nil, subscription, module, consumer_opts, state)
    end)
  end

  defp do_start_consumers(
         topic_name,
         nil,
         subscription,
         module,
         consumer_opts,
         %{cluster: cluster} = state
       ) do
    with {:ok, {%Topic{}, partitions}} <- PartitionManager.lookup(cluster, topic_name) do
      partitions = if partitions > 0, do: 0..(partitions - 1), else: [nil]

      do_start_consumers(
        topic_name,
        partitions,
        subscription,
        module,
        consumer_opts,
        state
      )
    else
      err ->
        Logger.error(
          "Error looking up topic partitions for topic #{topic_name}, on cluster #{cluster}, #{inspect(err)}"
        )

        err
    end
  end

  defp do_start_consumers(
         topic_name,
         partitions,
         subscription,
         module,
         consumer_opts,
         state
       ) do
    partitions
    |> Enum.reduce(nil, fn partition, err ->
      err ||
        do_start_consumer(
          topic_name,
          partition,
          subscription,
          module,
          consumer_opts,
          state
        )
    end)
  end

  defp do_stop_consumer(topic_name, partition, subscription, %{cluster: cluster}) do
    case Registry.lookup(ConsumerRegistry, {cluster, topic_name, partition, subscription}) do
      [] -> :ok
      [{consumer, _}] -> DynamicSupervisor.terminate_child(consumers(cluster), consumer)
    end
  end

  defp do_stop_consumers(topic_names, subscription, state) do
    topic_names
    |> Enum.each(fn topic_name ->
      do_stop_consumers(topic_name, nil, subscription, state)
    end)
  end

  defp do_stop_consumers(topic_name, nil, subscription, %{cluster: cluster} = state) do
    with {:ok, {%Topic{}, partitions}} <- PartitionManager.lookup(cluster, topic_name) do
      partitions = if partitions > 0, do: 0..(partitions - 1), else: [nil]
      do_stop_consumers(topic_name, partitions, subscription, state)
    else
      err ->
        Logger.error(
          "Error looking up topic partitions for topic #{topic_name}, on cluster #{cluster}, #{inspect(err)}"
        )
    end
  end

  defp do_stop_consumers(topic_name, partitions, subscription, state) do
    partitions
    |> Enum.each(fn partition ->
      do_stop_consumer(topic_name, partition, subscription, state)
    end)
  end

  defp name(cluster), do: String.to_atom("#{__MODULE__}.#{cluster}")
end
