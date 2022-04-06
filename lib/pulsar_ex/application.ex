defmodule PulsarEx.Application do
  @moduledoc false

  use Application

  @tab :pulsar_globals
  @num_executors 5

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

    num_executors = Application.get_env(:pulsar_ex, :num_executors, @num_executors)
    executors_overflow = round(num_executors * 0.2)

    executors_opts = [
      name: {:local, :pulsar_ex_executors},
      worker_module: PulsarEx.Executor,
      size: num_executors,
      max_overflow: executors_overflow,
      strategy: :fifo
    ]

    children = [
      :poolboy.child_spec(:pulsar_ex_executors, executors_opts),
      PartitionManager,
      ConnectionSupervisor,
      ProducerSupervisor,
      ConsumerSupervisor
    ]

    opts = [strategy: :rest_for_one, name: PulsarEx.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
