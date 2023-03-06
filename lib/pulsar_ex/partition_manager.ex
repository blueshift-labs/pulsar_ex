defmodule PulsarEx.PartitionManager do
  use GenServer

  alias PulsarEx.{Topic, Broker, Backoff, ConnectionManager, Connection}

  require Logger

  @partitions :pulsar_partitions

  @watch_interval 60_000

  @timeout 5000
  @max_attempts 5
  @backoff_type :rand_exp
  @backoff_min 500
  @backoff_max 5000
  @backoff Backoff.new(
             backoff_type: @backoff_type,
             backoff_min: @backoff_min,
             backoff_max: @backoff_max
           )

  def lookup(cluster_name, topic_name, state \\ %{attempts: 0, backoff: @backoff, error: nil})

  def lookup(_cluster_name, _topic_name, %{attempts: @max_attempts, error: err}),
    do: {:error, err}

  def lookup(cluster_name, topic_name, %{attempts: attempts, backoff: backoff}) do
    case do_lookup(cluster_name, topic_name) do
      {:ok, {topic, partitions}} ->
        {:ok, {topic, partitions}}

      {:error, err} ->
        Logger.error(
          "Error looking up partitions for topic [#{topic_name}], on cluster [#{cluster_name}], #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :partitions, :lookup, :error],
          %{count: 1},
          %{cluster: cluster_name, topic: topic_name}
        )

        {wait, backoff} = Backoff.backoff(backoff)
        Process.sleep(wait)
        lookup(cluster_name, topic_name, %{attempts: attempts + 1, backoff: backoff, error: err})
    end
  end

  defp do_lookup(cluster_name, topic_name) do
    case :ets.lookup(@partitions, {cluster_name, topic_name}) do
      [{{^cluster_name, ^topic_name}, {topic, partitions}}] ->
        {:ok, {topic, partitions}}

      [] ->
        deadline = System.monotonic_time(:millisecond) + @timeout
        timeout = 2 * @timeout
        GenServer.call(__MODULE__, {:lookup, {cluster_name, topic_name, deadline}}, timeout)
    end
  end

  def start_link(clusters) do
    GenServer.start_link(__MODULE__, clusters, name: __MODULE__)
  end

  def init(clusters) do
    Logger.debug("Starting partition manager")

    :ets.new(@partitions, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, Enum.into(clusters, %{}, &{&1.cluster_name, &1})}
  end

  def handle_call(
        {:lookup, {cluster_name, topic_name, deadline}},
        _from,
        state
      ) do
    with %{brokers: brokers, port: port} <- Map.get(state, cluster_name),
         [] <- :ets.lookup(@partitions, {cluster_name, topic_name}),
         {:ok, %Topic{} = topic} <- Topic.parse(topic_name),
         {:ok, partitions} <- internal_lookup(cluster_name, brokers, port, topic, deadline) do
      :ets.insert(@partitions, {{cluster_name, topic_name}, {topic, partitions}})

      if partitions > 0 do
        Process.send_after(
          self(),
          {:watch, {cluster_name, brokers, port, topic_name, topic}},
          @watch_interval + :rand.uniform(@watch_interval)
        )
      end

      {:reply, {:ok, {topic, partitions}}, state}
    else
      nil ->
        {:reply, {:error, :cluster_not_configured}, state}

      [{{^cluster_name, ^topic_name}, {topic, partitions}}] ->
        {:reply, {:ok, {topic, partitions}}, state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  def handle_info({:watch, {cluster_name, brokers, port, topic_name, topic}}, state) do
    deadline = System.monotonic_time(:millisecond) + @timeout

    case internal_lookup(cluster_name, brokers, port, topic, deadline) do
      {:ok, partitions} ->
        :ets.insert(@partitions, {{cluster_name, topic_name}, {topic, partitions}})

      {:error, err} ->
        Logger.error(
          "Error watching partitions for topic [#{topic_name}], on cluster [#{cluster_name}], #{inspect(err)}"
        )
    end

    Process.send_after(
      self(),
      {:watch, {cluster_name, brokers, port, topic_name, topic}},
      @watch_interval + :rand.uniform(@watch_interval)
    )

    {:noreply, state}
  end

  defp internal_lookup(cluster_name, brokers, port, topic, deadline) do
    broker_url = %Broker{host: Enum.random(brokers), port: port} |> to_string()

    with {:ok, conn} <- ConnectionManager.get_connection(cluster_name, broker_url) do
      Connection.lookup_topic_partitions(conn, to_string(topic), deadline)
    end
  end
end
