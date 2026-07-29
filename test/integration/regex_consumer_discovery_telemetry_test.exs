defmodule PulsarEx.RegexConsumerDiscoveryTelemetryTest do
  use ExUnit.Case, async: false

  alias PulsarEx.{TestRegexConsumer, TestAdmin, LoadTiming}

  @cluster "integration"
  @tenant "pulsar_ex"
  @namespace "IntegrationTest"

  @topic_a "persistent://pulsar_ex/IntegrationTest/RegexDiscoveryTelemetryTestA"
  @topic_b "persistent://pulsar_ex/IntegrationTest/RegexDiscoveryTelemetryTestB"
  @topic_regex ~r/^RegexDiscoveryTelemetryTest(A|B)$/

  @subscription "regex_consumer_discovery_telemetry_test"
  @refresh_interval 3_000

  @complete_event [:pulsar_ex, :consumer_manager, :discovery, :complete]

  setup do
    Application.put_env(:pulsar_ex, :admin_module, TestAdmin)
    TestAdmin.clear_overrides()

    test_pid = self()

    :telemetry.attach(
      "regex-consumer-discovery-telemetry-test",
      @complete_event,
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach("regex-consumer-discovery-telemetry-test")
      PulsarEx.Cluster.stop_consumer(@cluster, @tenant, @namespace, @topic_regex, @subscription)
      Application.delete_env(:pulsar_ex, :admin_module)
    end)

    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()
    TestAdmin.reset_subscription(@topic_a, @subscription)
    TestAdmin.reset_subscription(@topic_b, @subscription)

    {:ok, []}
  end

  test "a successful discovery pass emits duration, matched/desired/added/removed/failed counts" do
    TestAdmin.override(@tenant, @namespace, @topic_regex, {:ok, [@topic_a, @topic_b]})

    assert :ok =
             PulsarEx.Cluster.start_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               @subscription,
               TestRegexConsumer,
               refresh_interval: @refresh_interval
             )

    # The :complete event fires for every regex pattern in the whole test run, so the
    # subscription/phase must be matched directly in the receive pattern - otherwise an
    # unrelated pattern's refresh (running concurrently in another test file) could be
    # picked up instead and silently pass or fail on the wrong data.
    assert_receive {:telemetry_event, @complete_event, measurements,
                    %{subscription: @subscription, phase: :start} = metadata},
                   10_000

    assert %{
             matched_topics: 2,
             desired_partitions: 2,
             added: 2,
             removed: 0,
             failed_topics: 0
           } = measurements

    assert measurements.duration >= 0

    assert %{
             cluster: @cluster,
             tenant: @tenant,
             namespace: @namespace
           } = metadata

    assert String.length(metadata.pattern) <= 200

    # desired/active are populated synchronously by the time start_consumer returns
    # (snapshot written, ConsumerIDRegistry entries registered) - ready lags slightly
    # behind while each consumer's subscribe round-trip completes.
    assert %{desired: 2, active: 2} =
             PulsarEx.consumer_gauges(@cluster, @tenant, @namespace, @subscription)
             |> Map.take([:desired, :active])

    LoadTiming.wait_until(5_000, fn ->
      PulsarEx.consumer_gauges(@cluster, @tenant, @namespace, @subscription).ready == 2
    end)

    # A topic dropping out of discovery on the next refresh pass is reported as removed.
    TestAdmin.override(@tenant, @namespace, @topic_regex, {:ok, [@topic_a]})

    assert_receive {:telemetry_event, @complete_event, measurements,
                    %{subscription: @subscription, phase: :refresh}},
                   @refresh_interval + 5_000

    assert %{
             matched_topics: 1,
             desired_partitions: 1,
             added: 0,
             removed: 1,
             failed_topics: 0
           } = measurements
  end
end
