defmodule PulsarEx.ConsumerSupervisor do
  use Supervisor

  alias PulsarEx.{ConsumerRegistry, Consumers, ConsumerManager}

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :init, name: __MODULE__)
  end

  @impl true
  def init(:init) do
    consumer_opts = Application.get_env(:pulsar_ex, :consumer_opts, [])
    auto_start = Keyword.get(consumer_opts, :auto_start, true)

    children = [
      {Registry, keys: :unique, name: ConsumerRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: Consumers},
      {ConsumerManager, auto_start}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
