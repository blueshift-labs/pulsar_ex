defmodule PulsarEx.ConsumerSupervisor do
  use Supervisor

  alias PulsarEx.{ConsumerRegistry, Consumers, ConsumerManager}

  @partitions_lookup :pulsar_consumer_partitions

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

    consumer_opts = Application.get_env(:pulsar_ex, :consumer_opts, [])
    auto_start = Keyword.get(consumer_opts, :auto_start, true)

    children = [
      {Registry, keys: :unique, name: ConsumerRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: Consumers},
      {ConsumerManager, {@partitions_lookup, auto_start}}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
