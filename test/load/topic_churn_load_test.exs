defmodule PulsarEx.TopicChurnLoadTest do
  use ExUnit.Case

  alias PulsarEx.{
    LoadTopics,
    LoadChurnDriver,
    LoadRegexConsumer,
    LoadTiming,
    LoadTelemetryCounter,
    ConsumerRegistry
  }

  # Single-partition partitioned topics throughout, so every topic's
  # ConsumerRegistry key has exactly one deterministic partition suffix
  # (`-partition-0`) to check reconciliation against.
  @partitions 1
  @num_consumers 1

  @baseline_count 2
  @create_count 10
  @delete_count 3

  @duration_ms 5_000
  @refresh_interval 1_000

  @active_timeout 30_000
  @window_timeout 30_000

  @moduletag timeout: :infinity

  test "sustained topic creation/deletion churn: new topics get discovered, deleted topics get reconciled, no discovery errors" do
    prefix = "TopicChurn#{System.unique_integer([:positive])}_"
    topic_regex = Regex.compile!("^#{prefix}\\d+$")

    on_exit(fn -> LoadRegexConsumer.stop(topic_regex) end)

    LoadRegexConsumer.reset(topic_regex)

    # Baseline topics exist before the consumer starts, so discovery has
    # something to find on its very first pass, independent of anything churn
    # creates afterward. Churn topics use a disjoint index range (offset
    # @baseline_count) so the two sets never collide.
    baseline_topics = LoadTopics.provision(prefix, @baseline_count, @partitions)

    telemetry_handle = LoadTelemetryCounter.start()
    run_start = System.monotonic_time(:millisecond)

    assert :ok =
             LoadRegexConsumer.start(topic_regex,
               num_consumers: @num_consumers,
               refresh_interval: @refresh_interval
             )

    churn_result =
      LoadChurnDriver.run(
        prefix: prefix,
        partitions: @partitions,
        create_count: @create_count,
        delete_count: @delete_count,
        duration_ms: @duration_ms,
        offset: @baseline_count
      )

    still_alive_topics = baseline_topics ++ (churn_result.created -- churn_result.deleted)
    expected_consumers = length(still_alive_topics) * @partitions * @num_consumers

    active_done = LoadRegexConsumer.wait_until_active(expected_consumers, @active_timeout)

    received_before = LoadRegexConsumer.received()

    for topic <- still_alive_topics do
      assert {:ok, _} =
               PulsarEx.Cluster.produce(LoadRegexConsumer.cluster(), topic, "churn probe")
    end

    window_done =
      LoadTiming.wait_until(@window_timeout, fn ->
        LoadRegexConsumer.received() - received_before >= length(still_alive_topics)
      end)

    # Give every deleted topic one more refresh interval past the last delete
    # event before asserting its consumer is actually gone, matching the
    # documented "reconciled on the next refresh" contract (Phase 3 Task 3.4).
    Process.sleep(@refresh_interval + 2_000)

    for topic <- churn_result.deleted do
      assert [] =
               Registry.lookup(
                 ConsumerRegistry,
                 {LoadRegexConsumer.cluster(), "#{topic}-partition-0",
                  LoadRegexConsumer.subscription()}
               )
    end

    for topic <- still_alive_topics do
      assert [_ | _] =
               Registry.lookup(
                 ConsumerRegistry,
                 {LoadRegexConsumer.cluster(), "#{topic}-partition-0",
                  LoadRegexConsumer.subscription()}
               )
    end

    telemetry_tallies = LoadTelemetryCounter.stop(telemetry_handle)

    refute Map.has_key?(telemetry_tallies, [:pulsar_ex, :consumer_manager, :discovery, :error])

    # Force-deleting a topic out from under its live subscription (see
    # LoadChurnDriver's moduledoc) is expected to surface as a per-consumer
    # connect error on that topic's own consumer, at most once per deleted
    # topic - not a discovery-layer failure. More than that, or any error
    # bucket beyond this, would be an actual finding.
    connect_error_count =
      get_in(telemetry_tallies, [[:pulsar_ex, :consumer, :connect, :error], :count]) || 0

    assert connect_error_count <= length(churn_result.deleted)

    LoadRegexConsumer.stop(topic_regex)

    result = %{
      baseline_count: @baseline_count,
      create_count: @create_count,
      delete_count: @delete_count,
      still_alive_count: length(still_alive_topics),
      expected_consumers: expected_consumers,
      timing: %{
        time_to_active_ms: active_done - run_start,
        time_to_probe_received_ms: window_done - run_start
      },
      telemetry_tallies: LoadTelemetryCounter.json_safe(telemetry_tallies)
    }

    File.mkdir_p!("test/load/results")
    timestamp = System.system_time(:second)
    path = "test/load/results/topic_churn-#{timestamp}.json"
    File.write!(path, Jason.encode!(result, pretty: true))

    assert File.exists?(path)
  end
end
