defmodule PulsarEx.RegexConsumerRefreshTelemetryTest do
  use ExUnit.Case, async: false

  alias PulsarEx.TestRegexConsumer
  alias PulsarEx.TestAdmin

  @tenant "pulsar_ex"
  @namespace "IntegrationTest"
  @topic_regex ~r/^RegexRefreshTelemetryTest$/
  @event [:pulsar_ex, :consumer_manager, :discovery, :error]

  setup do
    Application.put_env(:pulsar_ex, :admin_module, TestAdmin)
    TestAdmin.clear_overrides()

    test_pid = self()

    :telemetry.attach(
      "regex-consumer-refresh-telemetry-test",
      @event,
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach("regex-consumer-refresh-telemetry-test")
      Application.delete_env(:pulsar_ex, :admin_module)
    end)

    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()

    {:ok, []}
  end

  test "a refresh-time discovery failure logs and emits telemetry" do
    assert :ok =
             PulsarEx.Cluster.start_consumer(
               "integration",
               @tenant,
               @namespace,
               @topic_regex,
               "regex_consumer_refresh_telemetry_test",
               TestRegexConsumer,
               refresh_interval: 3_000
             )

    TestAdmin.override(@tenant, @namespace, @topic_regex, {:error, :simulated_admin_down})

    assert_receive {:telemetry_event, @event, measurements, metadata}, 10_000

    assert %{count: 1} = measurements
    assert measurements.duration >= 0

    assert %{
             cluster: "integration",
             tenant: @tenant,
             namespace: @namespace,
             subscription: "regex_consumer_refresh_telemetry_test",
             phase: :refresh
           } = metadata

    assert metadata.pattern =~ "RegexRefreshTelemetryTest"
  end
end
