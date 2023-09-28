defmodule PulsarEx.ConnectionManager do
  use Connection

  alias PulsarEx.{Cluster, Broker, Backoff, ConnectionRegistry, ConnectionSupervisor}

  require Logger

  @num_connections 1
  @max_attempts 3
  @backoff_type :rand_exp
  @backoff_min 500
  @backoff_max 5000
  @backoff Backoff.new(
             backoff_type: @backoff_type,
             backoff_min: @backoff_min,
             backoff_max: @backoff_max
           )

  @timeout 30_000

  def get_connection(cluster_name, broker_url) do
    with [] <- Registry.lookup(ConnectionRegistry, {cluster_name, broker_url}),
         {:ok, connection} <-
           GenServer.call(__MODULE__, {:create, cluster_name, broker_url}, @timeout) do
      {:ok, connection}
    else
      [{pool, _}] ->
        {:ok, :poolboy.transaction(pool, & &1)}

      err ->
        err
    end
  end

  def get_cluster(cluster_name) do
    GenServer.call(__MODULE__, {:cluster, cluster_name})
  end

  def child_spec(clusters) do
    %{
      id: __MODULE__,
      type: :worker,
      start: {__MODULE__, :start_link, [clusters]}
    }
  end

  def start_link(clusters) do
    Connection.start_link(__MODULE__, clusters, name: __MODULE__)
  end

  @impl true
  def init(clusters) do
    Logger.debug("Starting connection manager")

    Process.flag(:trap_exit, true)

    clusters = Enum.into(clusters, %{}, &{&1.cluster_name, &1})

    {:connect, :init, %{clusters: clusters, attempts: 0, backoff: @backoff, error: nil}}
  end

  @impl true
  def connect(_, %{attempts: @max_attempts, error: err} = state) do
    {:stop, err, state}
  end

  def connect(_, %{clusters: clusters, attempts: attempts, backoff: backoff} = state) do
    connected =
      clusters
      |> Enum.flat_map(fn {_, %Cluster{brokers: brokers, port: port} = cluster} ->
        Enum.map(brokers, &{cluster, %Broker{host: &1, port: port}})
      end)
      |> Enum.reduce_while(:ok, fn
        {%{auto_connect: false}, _}, :ok ->
          {:cont, :ok}

        {cluster, broker}, :ok ->
          case start_connection(cluster, broker) do
            {:ok, _connection} ->
              {:cont, :ok}

            {:error, err} ->
              {:halt, {:error, err}}
          end
      end)

    case connected do
      :ok ->
        {:ok, %{clusters: clusters, attempts: 0, backoff: @backoff, error: nil}}

      {:error, _} = err ->
        {wait, backoff} = Backoff.backoff(backoff)

        {:backoff, wait, %{state | attempts: attempts + 1, backoff: backoff, error: err}}
    end
  end

  @impl true
  def handle_call({:create, cluster_name, broker_url}, _from, %{clusters: clusters} = state) do
    with %Cluster{} = cluster <- Map.get(clusters, cluster_name),
         {:ok, broker} <- Broker.parse(broker_url),
         {:ok, connection} <- start_connection(cluster, broker) do
      {:reply, {:ok, connection}, state}
    else
      nil ->
        {:reply, {:error, :cluster_not_configured}, state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  def handle_call({:cluster, cluster_name}, _from, %{clusters: clusters} = state) do
    {:reply, Map.get(clusters, cluster_name), state}
  end

  @impl true
  def terminate(reason, state) do
    case reason do
      :shutdown ->
        Logger.debug("Shutting down Connection Manager, #{inspect(reason)}")

      :normal ->
        Logger.debug("Shutting down Connection Manager, #{inspect(reason)}")

      {:shutdown, _} ->
        Logger.debug("Shutting down Connection Manager, #{inspect(reason)}")

      _ ->
        Logger.error("Shutting down Connection Manager, #{inspect(reason)}")
    end

    state
  end

  defp start_connection(%Cluster{} = cluster, %Broker{} = broker) do
    case Registry.lookup(ConnectionRegistry, {to_string(cluster), to_string(broker)}) do
      [{pool, _}] ->
        {:ok, :poolboy.transaction(pool, & &1)}

      [] ->
        case DynamicSupervisor.start_child(ConnectionSupervisor, pool_spec(cluster, broker)) do
          {:error, {:already_started, pool}} ->
            {:ok, :poolboy.transaction(pool, & &1)}

          {:ok, pool} ->
            Logger.debug("Connections started to broker", cluster: cluster, broker: broker)
            {:ok, :poolboy.transaction(pool, & &1)}

          {:error, err} ->
            Logger.error("Error starting connection to broker, #{inspect(err)}",
              cluster: cluster,
              broker: broker
            )

            {:error, err}
        end
    end
  end

  defp pool_spec(%Cluster{cluster_opts: cluster_opts} = cluster, %Broker{} = broker) do
    {num_connections, cluster_opts} =
      Keyword.pop(cluster_opts, :num_connections, @num_connections)

    {
      {to_string(cluster), to_string(broker)},
      {
        :poolboy,
        :start_link,
        [
          [
            name:
              {:via, Registry,
               {ConnectionRegistry, {to_string(cluster), to_string(broker)}, cluster_opts}},
            worker_module: connection_module(),
            size: num_connections,
            max_overflow: 0,
            strategy: :fifo
          ],
          {cluster, broker}
        ]
      },
      :permanent,
      :infinity,
      :supervisor,
      [:poolboy, connection_module()]
    }
  end

  defp connection_module() do
    Application.get_env(:pulsar_ex, :connection_module, PulsarEx.Connection)
  end
end
