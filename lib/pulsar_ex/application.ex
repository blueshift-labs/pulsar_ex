defmodule PulsarEx.Application do
  @moduledoc false

  use Application

  @counters :pulsar_counters

  alias PulsarEx.{
    PartitionManager,
    ConnectionSupervisor,
    ProducerSupervisor,
    ConsumerSupervisor,
    SignalHandler
  }

  def producer_id() do
    :ets.update_counter(@counters, :producer_id, {2, 1}, {:producer_id, 0})
  end

  def consumer_id() do
    :ets.update_counter(@counters, :consumer_id, {2, 1}, {:consumer_id, 0})
  end

  @impl true
  def start(_type, _args) do
    :ets.new(@counters, [
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
