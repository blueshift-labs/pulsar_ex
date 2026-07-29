defmodule PulsarEx.RegexConsumerDiscoveryTest do
  use ExUnit.Case, async: false

  alias PulsarEx.TestRegexConsumer
  alias PulsarEx.TestAdmin

  @tenant "pulsar_ex"
  @namespace "IntegrationTest"
  @topic "persistent://pulsar_ex/IntegrationTest/RegexConsumerDiscoveryTest"
  @topic_regex ~r/^RegexConsumerDiscoveryTest$/
  @event [:pulsar_ex, :consumer_manager, :discovery, :error]

  setup do
    Application.put_env(:pulsar_ex, :admin_module, TestAdmin)
    TestAdmin.clear_overrides()

    test_pid = self()

    :telemetry.attach(
      "regex-consumer-discovery-test",
      @event,
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach("regex-consumer-discovery-test")
      Application.delete_env(:pulsar_ex, :admin_module)
    end)

    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()
    TestAdmin.reset_subscription(@topic, "regex_consumer_discovery_success_test")
    TestAdmin.reset_subscription(@topic, "regex_consumer_discovery_failure_test")

    {:ok, []}
  end

  @tag timeout: :infinity
  test "discovery falls through to the real Admin module when no override is set" do
    for i <- 1..10 do
      assert {:ok, _} = PulsarEx.Cluster.produce("integration", @topic, "message #{i}")
    end

    assert :ok =
             PulsarEx.Cluster.start_consumer(
               "integration",
               @tenant,
               @namespace,
               @topic_regex,
               "regex_consumer_discovery_success_test",
               TestRegexConsumer,
               initial_position: :earliest
             )

    Process.sleep(10_000)

    assert [received: 10] = TestRegexConsumer.received()
  end

  test "discovery surfaces an injected Admin failure to the start_consumer caller and emits telemetry" do
    TestAdmin.override(@tenant, @namespace, @topic_regex, {:error, :simulated_admin_down})

    assert {:error, :simulated_admin_down} =
             PulsarEx.Cluster.start_consumer(
               "integration",
               @tenant,
               @namespace,
               @topic_regex,
               "regex_consumer_discovery_failure_test",
               TestRegexConsumer,
               initial_position: :earliest
             )

    assert_receive {:telemetry_event, @event, %{count: 1}, metadata}, 1_000

    assert %{
             cluster: "integration",
             tenant: @tenant,
             namespace: @namespace,
             subscription: "regex_consumer_discovery_failure_test"
           } = metadata

    assert metadata.pattern =~ "RegexConsumerDiscoveryTest"
  end
end
