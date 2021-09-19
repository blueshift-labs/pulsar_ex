defmodule PulsarEx.ConnectionSupervisor do
  use Supervisor

  alias PulsarEx.{ConnectionRegistry, ConnectionManager, Connections}

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :init, name: __MODULE__)
  end

  @impl true
  def init(:init) do
    children = [
      {Registry, keys: :unique, name: ConnectionRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: Connections},
      ConnectionManager
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
