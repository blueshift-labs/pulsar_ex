defmodule PulsarEx.ConsumerManager do
  defmodule State do
    @enforce_keys [
      :brokers,
      :admin_port,
      :refs
    ]

    defstruct [
      :brokers,
      :admin_port,
      :refs
    ]
  end

  use GenServer

  require Logger

  alias PulsarEx.{Consumers, Consumer, ConsumerRegistry, PartitionManager, Topic, Admin}

  @num_consumers 1
  @refresh_interval 60_000
  @connection_timeout 60_000

  def start_consumer(tenant, namespace, %Regex{} = regex, subscription, module, opts) do
    GenServer.call(
      __MODULE__,
      {:start, tenant, namespace, regex, subscription, module, opts},
      @connection_timeout
    )
  end

  def start_consumer(tenant, namespace, regex, subscription, module, opts) do
    start_consumer(tenant, namespace, Regex.compile!(regex), subscription, module, opts)
  end

  def start_consumer(topic_name, subscription, module, opts) do
    GenServer.call(
      __MODULE__,
      {:start, topic_name, nil, subscription, module, opts},
      @connection_timeout
    )
  end

  def start_consumer(topic_name, partitions, subscription, module, opts) do
    GenServer.call(
      __MODULE__,
      {:start, topic_name, partitions, subscription, module, opts},
      @connection_timeout
    )
  end

  def stop_consumer(tenant, namespace, regex, subscription) do
    GenServer.call(__MODULE__, {:stop, tenant, namespace, regex, subscription})
  end

  def stop_consumer(topic_name, subscription) do
    GenServer.call(__MODULE__, {:stop, topic_name, nil, subscription})
  end

  def stop_consumer(topic_name, partitions, subscription) do
    GenServer.call(__MODULE__, {:stop, topic_name, partitions, subscription})
  end

  def idle_time(topic_name, subscription) do
    with {:ok, {%Topic{}, partitions}} <- PartitionManager.lookup(topic_name) do
      partitions = if partitions > 0, do: 0..(partitions - 1), else: [nil]

      partitions
      |> Enum.flat_map(fn partition ->
        Registry.lookup(ConsumerRegistry, {topic_name, partition, subscription})
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

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    Logger.debug("Starting consumer manager")

    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    Process.flag(:trap_exit, true)

    Application.get_env(:pulsar_ex, :consumers, [])
    |> Enum.each(fn opts ->
      subscription = Keyword.fetch!(opts, :subscription)
      module = Keyword.fetch!(opts, :module)

      case Keyword.get(opts, :regex) do
        nil ->
          topic_name = Keyword.fetch!(opts, :topic)
          partitions = Keyword.get(opts, :partitions, nil)
          Process.send(self(), {:start, topic_name, partitions, subscription, module, opts}, [])

        regex ->
          tenant = Keyword.fetch!(opts, :tenant)
          namespace = Keyword.fetch!(opts, :namespace)
          regex = Regex.compile!(regex)

          Process.send(self(), {:start, tenant, namespace, regex, subscription, module, opts}, [])
      end
    end)

    {:ok, %State{brokers: brokers, admin_port: admin_port, refs: %{}}}
  end

  @impl true
  def handle_call(
        {:start, tenant, namespace, regex, subscription, module, opts},
        _from,
        state
      ) do
    case Admin.discover_topics(state.brokers, state.admin_port, tenant, namespace, regex) do
      {:ok, topic_names} ->
        consumer_opts = consumer_opts(opts)
        auto_refresh = Keyword.get(consumer_opts, :auto_refresh, true)
        refresh_interval = Keyword.get(consumer_opts, :refresh_interval, @refresh_interval)

        case do_start_consumers(topic_names, subscription, module, consumer_opts) do
          nil ->
            if auto_refresh do
              ref =
                Process.send_after(
                  self(),
                  {:refresh, tenant, namespace, regex, subscription, module, opts},
                  refresh_interval + :rand.uniform(refresh_interval)
                )

              {:reply, :ok,
               %State{
                 state
                 | refs: Map.put(state.refs, {tenant, namespace, regex, subscription}, ref)
               }}
            else
              {:reply, :ok, state}
            end

          {:error, _} = err ->
            {:reply, err, state}
        end

      {:error, err} ->
        Logger.error("Error discovering topics for #{tenant}/#{namespace}, #{inspect(err)}")
        {:reply, {:error, err}, state}
    end
  end

  @impl true
  def handle_call(
        {:start, topic_name, nil, subscription, module, opts},
        _from,
        state
      ) do
    consumer_opts = consumer_opts(opts)
    auto_refresh = Keyword.get(consumer_opts, :auto_refresh, true)
    refresh_interval = Keyword.get(consumer_opts, :refresh_interval, @refresh_interval)

    case do_start_consumers(topic_name, nil, subscription, module, consumer_opts) do
      nil ->
        if auto_refresh do
          ref =
            Process.send_after(
              self(),
              {:refresh, topic_name, subscription, module, opts},
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
        {:start, topic_name, partitions, subscription, module, opts},
        _from,
        state
      ) do
    case do_start_consumers(topic_name, partitions, subscription, module, consumer_opts(opts)) do
      nil ->
        {:reply, :ok, state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  @impl true
  def handle_call({:stop, tenant, namespace, regex, subscription}, _from, state) do
    {ref, refs} = Map.pop(state.refs, {tenant, namespace, regex, subscription})

    if ref != nil do
      Process.cancel_timer(ref)
    end

    state = %State{state | refs: refs}

    case Admin.discover_topics(state.brokers, state.admin_port, tenant, namespace, regex) do
      {:ok, topic_names} ->
        do_stop_consumers(topic_names, subscription)
        {:reply, :ok, state}

      {:error, err} ->
        Logger.error("Error discovering topics for #{tenant}/#{namespace}, #{inspect(err)}")
        {:reply, {:error, err}, state}
    end
  end

  @impl true
  def handle_call({:stop, topic_name, nil, subscription}, _from, state) do
    {ref, refs} = Map.pop(state.refs, {topic_name, subscription})

    if ref != nil do
      Process.cancel_timer(ref)
    end

    do_stop_consumers(topic_name, nil, subscription)
    {:reply, :ok, %State{state | refs: refs}}
  end

  @impl true
  def handle_call({:stop, topic_name, partitions, subscription}, _from, state) do
    do_stop_consumers(topic_name, partitions, subscription)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:refresh, topic_name, subscription, module, opts}, state) do
    consumer_opts = consumer_opts(opts)
    refresh_interval = Keyword.get(consumer_opts, :refresh_interval, @refresh_interval)
    do_start_consumers(topic_name, nil, subscription, module, consumer_opts)

    ref =
      Process.send_after(
        self(),
        {:refresh, topic_name, subscription, module, opts},
        refresh_interval + :rand.uniform(refresh_interval)
      )

    {:noreply, %State{state | refs: Map.put(state.refs, {topic_name, subscription}, ref)}}
  end

  @impl true
  def handle_info({:refresh, tenant, namespace, regex, subscription, module, opts}, state) do
    consumer_opts = consumer_opts(opts)
    refresh_interval = Keyword.get(consumer_opts, :refresh_interval, @refresh_interval)

    case Admin.discover_topics(state.brokers, state.admin_port, tenant, namespace, regex) do
      {:ok, topic_names} ->
        do_start_consumers(topic_names, subscription, module, consumer_opts)

      {:error, err} ->
        Logger.error("Error discovering topics for #{tenant}/#{namespace}, #{inspect(err)}")
    end

    ref =
      Process.send_after(
        self(),
        {:refresh, tenant, namespace, regex, subscription, module, opts},
        refresh_interval + :rand.uniform(refresh_interval)
      )

    {:noreply,
     %State{state | refs: Map.put(state.refs, {tenant, namespace, regex, subscription}, ref)}}
  end

  @impl true
  def terminate(reason, state) do
    case reason do
      :shutdown ->
        Logger.info("Shutting down Consumer Manager, #{inspect(reason)}")

      :normal ->
        Logger.info("Shutting down Consumer Manager, #{inspect(reason)}")

      {:shutdown, _} ->
        Logger.info("Shutting down Consumer Manager, #{inspect(reason)}")

      _ ->
        Logger.error("Shutting down Consumer Manager, #{inspect(reason)}")
    end

    state
  end

  defp consumer_opts(opts) do
    Keyword.merge(
      Application.get_env(:pulsar_ex, :consumer_opts, []),
      opts,
      fn
        :properties, v1, v2 -> Keyword.merge(v1, v2)
        _, _, v -> v
      end
    )
  end

  defp consumer_spec(topic_name, partition, subscription, module, consumer_opts) do
    consumers = Keyword.get(consumer_opts, :num_consumers, @num_consumers)

    children =
      for n <- 0..(consumers - 1) do
        Supervisor.child_spec({module, {topic_name, partition, subscription, consumer_opts}},
          id: {module, n}
        )
      end

    %{
      id: {topic_name, partition, subscription},
      start: {
        Supervisor,
        :start_link,
        [
          children,
          [
            strategy: :one_for_one,
            name: {:via, Registry, {ConsumerRegistry, {topic_name, partition, subscription}}}
          ]
        ]
      },
      restart: :permanent,
      shutdown: :infinity,
      type: :supervisor,
      modules: [module]
    }
  end

  defp do_start_consumer(topic_name, partition, subscription, module, consumer_opts) do
    Logger.debug(
      "Starting consumer for topic #{topic_name}, partition #{partition} with subscription #{subscription}"
    )

    DynamicSupervisor.start_child(
      Consumers,
      consumer_spec(topic_name, partition, subscription, module, consumer_opts)
    )
    |> case do
      {:ok, _} ->
        Logger.debug(
          "Started consumer for topic #{topic_name}, partition #{partition} with subscription #{subscription}"
        )

        nil

      {:error, {:already_started, _}} ->
        nil

      {:error, err} ->
        Logger.error(
          "Error starting consumer for topic #{topic_name}, partition #{partition} with subscription #{subscription}, #{inspect(err)}"
        )

        {:error, err}
    end
  end

  defp do_start_consumers(topic_names, subscription, module, consumer_opts) do
    topic_names
    |> Enum.reduce(nil, fn topic_name, err ->
      err || do_start_consumers(topic_name, nil, subscription, module, consumer_opts)
    end)
  end

  defp do_start_consumers(topic_name, nil, subscription, module, consumer_opts) do
    with {:ok, {%Topic{}, partitions}} <- PartitionManager.lookup(topic_name) do
      partitions = if partitions > 0, do: 0..(partitions - 1), else: [nil]
      do_start_consumers(topic_name, partitions, subscription, module, consumer_opts)
    else
      err ->
        Logger.error("Error looking up topic partitions for topic #{topic_name}, #{inspect(err)}")
        err
    end
  end

  defp do_start_consumers(topic_name, partitions, subscription, module, consumer_opts) do
    partitions
    |> Enum.reduce(nil, fn partition, err ->
      err || do_start_consumer(topic_name, partition, subscription, module, consumer_opts)
    end)
  end

  defp do_stop_consumer(topic_name, partition, subscription) do
    case Registry.lookup(ConsumerRegistry, {topic_name, partition, subscription}) do
      [] -> :ok
      [{consumer, _}] -> DynamicSupervisor.terminate_child(Consumers, consumer)
    end
  end

  defp do_stop_consumers(topic_names, subscription) do
    topic_names
    |> Enum.each(fn topic_name ->
      do_stop_consumers(topic_name, nil, subscription)
    end)
  end

  defp do_stop_consumers(topic_name, nil, subscription) do
    with {:ok, {%Topic{}, partitions}} <- PartitionManager.lookup(topic_name) do
      partitions = if partitions > 0, do: 0..(partitions - 1), else: [nil]
      do_stop_consumers(topic_name, partitions, subscription)
    else
      err ->
        Logger.error("Error looking up topic partitions for topic #{topic_name}, #{inspect(err)}")
    end
  end

  defp do_stop_consumers(topic_name, partitions, subscription) do
    partitions
    |> Enum.each(fn partition ->
      do_stop_consumer(topic_name, partition, subscription)
    end)
  end
end
