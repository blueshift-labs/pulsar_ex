defmodule PulsarEx.ConsumerManager do
  use GenServer

  require Logger

  alias PulsarEx.{Consumers, ConsumerRegistry, PartitionManager, Topic}

  @num_consumers 1
  @refresh_interval 60_000

  def start_consumer(topic_name, subscription, module, opts) do
    GenServer.call(__MODULE__, {:start, topic_name, subscription, module, opts}, :infinity)
  end

  def start_consumer(topic_name, partitions, subscription, module, opts) do
    GenServer.call(
      __MODULE__,
      {:start, topic_name, partitions, subscription, module, opts},
      :infinity
    )
  end

  def stop_consumer(topic_name, subscription) do
    GenServer.call(__MODULE__, {:stop, topic_name, subscription}, :infinity)
  end

  def stop_consumer(topic_name, partitions, subscription) do
    GenServer.call(__MODULE__, {:stop, topic_name, partitions, subscription}, :infinity)
  end

  def start_link(auto_start) do
    GenServer.start_link(__MODULE__, auto_start, name: __MODULE__)
  end

  @impl true
  def init(false) do
    Logger.debug("Starting consumer manager")
    Process.flag(:trap_exit, true)
    {:ok, %{}}
  end

  @impl true
  def init(true) do
    Logger.debug("Starting consumer manager")
    Process.flag(:trap_exit, true)

    Application.get_env(:pulsar_ex, :consumers, [])
    |> Enum.reduce_while({:ok, %{}}, fn opts, {:ok, refs} ->
      topic_name = Keyword.fetch!(opts, :topic)
      partitions = Keyword.get(opts, :partitions, nil)
      subscription = Keyword.fetch!(opts, :subscription)
      module = Keyword.fetch!(opts, :module)
      consumer_opts = consumer_opts(opts)
      refresh_interval = Keyword.get(consumer_opts, :refresh_interval, @refresh_interval)
      auto_refresh = Keyword.get(consumer_opts, :auto_refresh, true)

      case do_start_consumers(topic_name, partitions, subscription, module, consumer_opts) do
        nil ->
          if partitions == nil && auto_refresh do
            ref =
              Process.send_after(
                self(),
                {:refresh, topic_name, subscription, module, opts},
                refresh_interval + :rand.uniform(refresh_interval)
              )

            {:cont, {:ok, Map.put(refs, {topic_name, subscription}, ref)}}
          else
            {:cont, {:ok, refs}}
          end

        err ->
          {:halt, err}
      end
    end)
    |> case do
      {:ok, state} -> {:ok, state}
      err -> {:stop, err}
    end
  end

  @impl true
  def handle_call(
        {:start, topic_name, subscription, module, opts},
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

          {:reply, :ok, Map.put(state, {topic_name, subscription}, ref)}
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
  def handle_call({:stop, topic_name, partitions, subscription}, _from, state) do
    do_stop_consumers(topic_name, partitions, subscription)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:stop, topic_name, subscription}, _from, state) do
    {ref, state} = Map.pop(state, {topic_name, subscription})

    if ref != nil do
      Process.cancel_timer(ref)
    end

    do_stop_consumers(topic_name, nil, subscription)
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

    {:noreply, Map.put(state, {topic_name, subscription}, ref)}
  end

  @impl true
  def terminate(reason, state) do
    Logger.error("Shutting down Consumer Manager, #{inspect(reason)}")
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
      "Starting consumer for topic #{topic_name}, partition #{partition} with subscription #{
        subscription
      }"
    )

    DynamicSupervisor.start_child(
      Consumers,
      consumer_spec(topic_name, partition, subscription, module, consumer_opts)
    )
    |> case do
      {:ok, _} ->
        Logger.debug(
          "Started consumer for topic #{topic_name}, partition #{partition} with subscription #{
            subscription
          }"
        )

        nil

      {:error, {:already_started, _}} ->
        nil

      {:error, err} ->
        Logger.error(
          "Error starting consumer for topic #{topic_name}, partition #{partition} with subscription #{
            subscription
          }, #{inspect(err)}"
        )

        {:error, err}
    end
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
