defmodule PulsarEx.Application do
  @moduledoc false

  use Application

  @tab :pulsar_globals

  alias PulsarEx.{
    PartitionManager,
    ConnectionSupervisor,
    ProducerSupervisor,
    ConsumerSupervisor,
    SignalHandler
  }

  def shutdown!() do
    :ets.insert(@tab, {:shutdown, true})
  end

  def shutdown?() do
    :ets.lookup(@tab, :shutdown) == [shutdown: true]
  end

  @impl true
  def start(_type, _args) do
    :ets.new(@tab, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    SignalHandler.start_link()

    children = [
      PartitionManager,
      ConnectionSupervisor,
      ProducerSupervisor,
      ConsumerSupervisor
    ]

    opts = [strategy: :rest_for_one, name: PulsarEx.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
