defmodule PulsarEx.ProducerSupervisor do
  use Supervisor

  alias PulsarEx.{ProducerRegistry, Producers, ProducerManager}

  @partitions_lookup :pulsar_producer_partitions

  def lookup_partitions(topic_name) do
    :ets.lookup(@partitions_lookup, topic_name)
  end

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :init, name: __MODULE__)
  end

  @impl true
  def init(:init) do
    :ets.new(@partitions_lookup, [
      :named_table,
      :set,
      :public,
      write_concurrency: true,
      read_concurrency: true
    ])

    producer_opts = Application.get_env(:pulsar_ex, :producer_opts, [])
    auto_start = Keyword.get(producer_opts, :auto_start, true)

    children = [
      {Registry, keys: :unique, name: ProducerRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: Producers},
      {ProducerManager, {@partitions_lookup, auto_start}}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
