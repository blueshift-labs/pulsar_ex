defmodule PulsarEx.PartitionedConsumerSupervisor do
  use Supervisor

  alias PulsarEx.PartitionedConsumerManager

  def start_link({topic_name, subscription, module, consumer_opts, lookup}) do
    Supervisor.start_link(__MODULE__, {topic_name, subscription, module, consumer_opts, lookup})
  end

  @impl true
  def init({topic_name, subscription, module, consumer_opts, lookup}) do
    sup = sup_name(topic_name)

    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: sup},
      {PartitionedConsumerManager, {topic_name, subscription, module, consumer_opts, lookup, sup}}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp sup_name(topic_name) do
    String.to_atom("pulsar_consumer_sup_#{topic_name}")
  end
end
