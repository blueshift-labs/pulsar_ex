defmodule PulsarEx.ConnectionManager do
  defmodule State do
    @enforce_keys [:brokers, :admin_port]

    defstruct [:brokers, :admin_port]
  end

  use GenServer

  alias PulsarEx.{Broker, Connections, Connection, ConnectionRegistry, Admin}

  require Logger

  @num_connections 1

  @health_check_interval 5000

  def get_connection(%Broker{} = broker) do
    with [] <- Registry.lookup(ConnectionRegistry, broker),
         {:ok, pool} <- GenServer.call(__MODULE__, {:create, broker}) do
      {:ok, pool}
    else
      [{pool, _}] ->
        {:ok, :poolboy.transaction(pool, & &1)}

      err ->
        err
    end
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :init, name: __MODULE__)
  end

  @impl true
  def init(:init) do
    Logger.debug("Starting connection manager")

    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    {:ok, %State{brokers: brokers, admin_port: admin_port}}
  end

  @impl true
  def handle_call({:create, %Broker{} = broker}, _from, state) do
    case start_connection(broker) do
      {:ok, pool} ->
        Process.send_after(
          self(),
          {:health_check, broker},
          @health_check_interval + :rand.uniform(@health_check_interval)
        )

        {:reply, {:ok, pool}, state}

      err ->
        {:reply, err, state}
    end
  end

  @impl true
  def handle_info({:health_check, %Broker{} = broker}, state) do
    case Admin.health_check(broker, state.admin_port) do
      :ok ->
        Logger.debug("Broker #{Broker.to_name(broker)} is healthy")

        Process.send_after(
          self(),
          {:health_check, broker},
          @health_check_interval + :rand.uniform(@health_check_interval)
        )

      err ->
        Logger.error(
          "Broker #{Broker.to_name(broker)} is not healthy, #{inspect(err)}, stopping..."
        )

        case Registry.lookup(ConnectionRegistry, broker) do
          [{pool, _}] -> DynamicSupervisor.terminate_child(Connections, pool)
          _ -> nil
        end
    end

    {:noreply, state}
  end

  defp start_connection(%Broker{} = broker) do
    case Registry.lookup(ConnectionRegistry, broker) do
      [{pool, _}] ->
        {:ok, :poolboy.transaction(pool, & &1)}

      [] ->
        Logger.debug("Starting connection for broker #{Broker.to_name(broker)}")

        case DynamicSupervisor.start_child(Connections, pool_spec(broker)) do
          {:error, {:already_started, pool}} ->
            Logger.debug("Connections already started for broker #{Broker.to_name(broker)}")
            {:ok, pool}

          {:ok, pool} ->
            Logger.debug("Connections started for broker #{Broker.to_name(broker)}")
            {:ok, pool}

          {:error, err} ->
            Logger.error(
              "Error starting connection for broker #{Broker.to_name(broker)}, #{inspect(err)}"
            )

            {:error, err}
        end
    end
  end

  defp pool_spec(%Broker{} = broker) do
    {
      Broker.to_name(broker),
      {
        :poolboy,
        :start_link,
        [
          [
            name: {:via, Registry, {ConnectionRegistry, broker}},
            worker_module: Connection,
            size: Application.get_env(:pulsar_ex, :num_connections, @num_connections),
            max_overflow: 0,
            strategy: :fifo
          ],
          broker
        ]
      },
      :permanent,
      :infinity,
      :supervisor,
      [:poolboy, Connection]
    }
  end
end
