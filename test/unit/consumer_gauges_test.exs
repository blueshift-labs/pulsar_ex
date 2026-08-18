defmodule PulsarEx.ConsumerGaugesTest do
  use ExUnit.Case

  defmodule TestConsumer do
    use PulsarEx.Consumer

    @impl PulsarEx.ConsumerCallback
    def handle_messages(messages, _state) do
      Enum.map(messages, fn _ -> :ok end)
    end
  end

  test "gauges track an exact-topic consumer through start, ready, and stop" do
    cluster_name = "unit"
    tenant = "pulsar_ex"
    namespace = "ConsumerGaugesTest"
    topic_name = "persistent://#{tenant}/#{namespace}/consumer_gauges_test"
    subscription = "test"

    on_exit(fn -> PulsarEx.Cluster.stop_consumer(cluster_name, topic_name, subscription) end)

    :ok =
      PulsarEx.Cluster.start_consumer(
        cluster_name,
        topic_name,
        subscription,
        TestConsumer,
        notify: {self(), :ignore},
        connect_delay: 300
      )

    assert PulsarEx.consumer_gauges(cluster_name, tenant, namespace, subscription) == %{
             desired: 1,
             active: 1,
             ready: 0
           }

    Process.sleep(500)

    assert PulsarEx.consumer_gauges(cluster_name, tenant, namespace, subscription) == %{
             desired: 1,
             active: 1,
             ready: 1
           }

    assert :ok = PulsarEx.Cluster.stop_consumer(cluster_name, topic_name, subscription)

    assert %{desired: 0} =
             PulsarEx.consumer_gauges(cluster_name, tenant, namespace, subscription)
  end
end
