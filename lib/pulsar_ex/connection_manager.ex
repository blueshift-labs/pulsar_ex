defmodule PulsarEx.ConnectionManager do
  defmodule State do
    @enforce_keys [
      :cluster,
      :cluster_opts,
      :admin_port,
      :health_check,
      :num_connections
    ]

    defstruct [
      :cluster,
      :cluster_opts,
      :admin_port,
      :health_check,
      :num_connections
    ]
  end

  use GenServer

  alias PulsarEx.{Broker, Connection, ConnectionRegistry, Admin}

  require Logger

  @num_connections 1
  @health_check_interval 5000
  @connection_timeout 60_000

  def connections(cluster), do: String.to_atom("PulsarEx.Connections.#{cluster}")

  def get_connection(cluster, %Broker{} = broker) do
    with [] <- Registry.lookup(ConnectionRegistry, {cluster, broker}),
         {:ok, pool} <- GenServer.call(name(cluster), {:create, broker}, @connection_timeout) do
      {:ok, pool}
    else
      [{pool, _}] ->
        {:ok, :poolboy.transaction(pool, & &1)}

      err ->
        err
    end
  end

  def start_link(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)
    GenServer.start_link(__MODULE__, cluster_opts, name: name(cluster))
  end

  @impl true
  def init(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)
    Logger.debug("Starting connection manager, on cluster #{cluster}")

    admin_port = Keyword.fetch!(cluster_opts, :admin_port)
    health_check = Keyword.get(cluster_opts, :health_check, false)
    num_connections = Keyword.get(cluster_opts, :num_connections, @num_connections)

    {:ok,
     %State{
       cluster: cluster,
       cluster_opts: cluster_opts,
       admin_port: admin_port,
       health_check: health_check,
       num_connections: num_connections
     }}
  end

  @impl true
  def handle_call({:create, %Broker{} = broker}, _from, state) do
    case start_connection(broker, state) do
      {:ok, pool} ->
        {:reply, {:ok, pool}, state}

      err ->
        {:reply, err, state}
    end
  end

  @impl true
  def handle_info({:health_check, %Broker{} = broker, pool}, %{cluster: cluster} = state) do
    case Admin.health_check(broker, state.admin_port) do
      :ok ->
        Logger.debug("Broker #{Broker.to_name(broker)} is healthy, on cluster #{cluster}")

        Process.send_after(
          self(),
          {:health_check, broker, pool},
          @health_check_interval + :rand.uniform(@health_check_interval)
        )

      err ->
        Logger.error(
          "Broker #{Broker.to_name(broker)} is not healthy, on cluster #{cluster}, #{inspect(err)}, stopping..."
        )

        DynamicSupervisor.terminate_child(connections(cluster), pool)
    end

    {:noreply, state}
  end

  defp start_connection(%Broker{} = broker, %{cluster: cluster} = state) do
    case Registry.lookup(ConnectionRegistry, {cluster, broker}) do
      [{pool, _}] ->
        {:ok, :poolboy.transaction(pool, & &1)}

      [] ->
        Logger.debug(
          "Starting connection to broker #{Broker.to_name(broker)}, on cluster #{cluster}"
        )

        case DynamicSupervisor.start_child(connections(cluster), pool_spec(broker, state)) do
          {:error, {:already_started, pool}} ->
            Logger.debug(
              "Connections already started to broker #{Broker.to_name(broker)}, on cluster #{cluster}"
            )

            {:ok, pool}

          {:ok, pool} ->
            Logger.debug(
              "Connections started to broker #{Broker.to_name(broker)}, on cluster #{cluster}"
            )

            if state.health_check do
              Process.send_after(
                self(),
                {:health_check, broker, pool},
                @health_check_interval + :rand.uniform(@health_check_interval)
              )
            end

            {:ok, pool}

          {:error, err} ->
            Logger.error(
              "Error starting connection to broker #{Broker.to_name(broker)}, on cluster #{cluster}, #{inspect(err)}"
            )

            {:error, err}
        end
    end
  end

  defp pool_spec(%Broker{} = broker, %{cluster: cluster} = state) do
    {
      {cluster, broker},
      {
        :poolboy,
        :start_link,
        [
          [
            name: {:via, Registry, {ConnectionRegistry, {cluster, broker}}},
            worker_module: Connection,
            size: state.num_connections,
            max_overflow: 0,
            strategy: :fifo
          ],
          {broker, state.cluster_opts}
        ]
      },
      :permanent,
      :infinity,
      :supervisor,
      [:poolboy, Connection]
    }
  end

  defp name(cluster), do: String.to_atom("#{__MODULE__}.#{cluster}")
end
