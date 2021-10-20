defmodule PulsarEx.ConnectionManager do
  defmodule State do
    @enforce_keys [:brokers, :admin_port]

    defstruct [:brokers, :admin_port]
  end

  use GenServer

  alias PulsarEx.{Broker, Connections, Connection, ConnectionRegistry}

  require Logger

  @num_connections 3

  def get_connection(%Broker{} = broker) do
    with [] <- Registry.lookup(ConnectionRegistry, broker),
         :ok <- GenServer.call(__MODULE__, {:create, broker}) do
      get_connection(broker)
    else
      [{pool, _}] -> {:ok, pool}
      err -> err
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
      :ok -> {:reply, :ok, state}
      err -> {:reply, err, state}
    end
  end

  defp start_connection(%Broker{} = broker) do
    Logger.debug("Starting connection for broker #{Broker.to_name(broker)}")

    case DynamicSupervisor.start_child(Connections, pool_spec(broker)) do
      {:error, {:already_started, _}} ->
        Logger.debug("Connections already started for broker #{Broker.to_name(broker)}")
        :ok

      {:ok, _} ->
        Logger.debug("Connections started for broker #{Broker.to_name(broker)}")
        :ok

      {:error, err} ->
        Logger.error(
          "Error starting connection for broker #{Broker.to_name(broker)}, #{inspect(err)}"
        )

        {:error, err}
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
