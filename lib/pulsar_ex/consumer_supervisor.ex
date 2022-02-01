defmodule PulsarEx.ConsumerSupervisor do
  use Supervisor

  alias PulsarEx.{ConsumerRegistry, Consumers, ConsumerManager}

  @max_restarts 100

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :init, name: __MODULE__)
  end

  @impl true
  def init(:init) do
    children = [
      {Registry, keys: :unique, name: ConsumerRegistry, partitions: System.schedulers_online()},
      {DynamicSupervisor, strategy: :one_for_one, max_restarts: @max_restarts, name: Consumers},
      ConsumerManager
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
