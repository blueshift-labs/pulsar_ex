defmodule PulsarEx.ProducerSupervisor do
  use Supervisor

  alias PulsarEx.{ProducerRegistry, Producers, ProducerManager}

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :init, name: __MODULE__)
  end

  @impl true
  def init(:init) do
    producer_opts = Application.get_env(:pulsar_ex, :producer_opts, [])
    auto_start = Keyword.get(producer_opts, :auto_start, true)

    children = [
      {Registry, keys: :unique, name: ProducerRegistry, partitions: System.schedulers_online()},
      {DynamicSupervisor, strategy: :one_for_one, name: Producers},
      {ProducerManager, auto_start}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
