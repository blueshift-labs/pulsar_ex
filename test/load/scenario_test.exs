defmodule PulsarEx.LoadScenarioTest do
  use ExUnit.Case

  alias PulsarEx.{
    LoadTopics,
    LoadRegexConsumer,
    LoadTiming,
    LoadAdmin,
    LoadResourceSampler,
    LoadTelemetryCounter,
    TestAdmin
  }

  @partition_count 5
  @num_consumers 1

  @messages_per_topic 5
  @peak_messages_per_topic 20
  @peak_max_concurrency 20

  @active_timeout 60_000
  @ready_timeout 60_000
  @window_timeout 60_000

  @sampler_interval_ms 500

  @moduletag timeout: :infinity

  @tag stage: 25
  test "stage: 25 topics x #{@partition_count} partitions" do
    run_stage(25)
  end

  @tag stage: 50
  test "stage: 50 topics x #{@partition_count} partitions" do
    run_stage(50)
  end

  @tag stage: 100
  test "stage: 100 topics x #{@partition_count} partitions" do
    run_stage(100)
  end

  # Own topic prefix per stage (see regex_consumer_stepped_load_test.exs) so
  # discovery regexes never cross-match another load test file's topics or
  # another stage's topics.
  defp run_stage(topic_count) do
    topic_prefix = "LoadScenario#{topic_count}_"
    topic_regex = Regex.compile!("^#{topic_prefix}\\d+$")

    on_exit(fn -> LoadRegexConsumer.stop(topic_regex) end)

    LoadRegexConsumer.reset(topic_regex)

    LoadAdmin.reset()

    topics = LoadTopics.provision(topic_prefix, topic_count, @partition_count, 0, LoadAdmin)

    for topic <- topics do
      TestAdmin.reset_subscription(topic, LoadRegexConsumer.subscription())
    end

    {:ok, sampler_pid} = LoadResourceSampler.start(@sampler_interval_ms)
    telemetry_handle = LoadTelemetryCounter.start()

    desired_consumers = topic_count * @partition_count * @num_consumers

    {start_result, call_start, call_done} =
      LoadTiming.time_call(fn ->
        LoadRegexConsumer.start(topic_regex, num_consumers: @num_consumers)
      end)

    assert :ok = start_result

    active_done = LoadRegexConsumer.wait_until_active(desired_consumers, @active_timeout)
    ready_done = LoadRegexConsumer.wait_until_ready(desired_consumers, @ready_timeout)

    expected_window = run_expected_window(topics)
    peak_window = run_peak_window(topics)

    broker_stats_summary = read_broker_stats_summary(topics)

    resource_samples = LoadResourceSampler.stop(sampler_pid)
    telemetry_tallies = LoadTelemetryCounter.stop(telemetry_handle)
    admin_stats = LoadAdmin.stats()

    LoadRegexConsumer.stop(topic_regex)

    result = %{
      topic_count: topic_count,
      partition_count: @partition_count,
      num_consumers: @num_consumers,
      desired_consumers: desired_consumers,
      timing: %{
        start_consumer_call_ms: call_done - call_start,
        time_to_active_ms: active_done - call_start,
        time_to_ready_ms: ready_done - call_start
      },
      expected_window: expected_window,
      peak_window: peak_window,
      resource_samples: LoadResourceSampler.json_safe(resource_samples),
      telemetry_tallies: LoadTelemetryCounter.json_safe(telemetry_tallies),
      admin_stats: admin_stats,
      broker_stats_summary: broker_stats_summary
    }

    File.mkdir_p!("test/load/results")
    timestamp = System.system_time(:second)
    path = "test/load/results/#{topic_count}x#{@partition_count}-#{timestamp}.json"
    File.write!(path, Jason.encode!(result, pretty: true))

    IO.puts("""

    === Load scenario result: #{topic_count} topics x #{@partition_count} partitions, num_consumers: #{@num_consumers} ===
    start_consumer call:             #{call_done - call_start} ms
    time to #{desired_consumers} active consumers: #{active_done - call_start} ms
    time to #{desired_consumers} ready consumers: #{ready_done - call_start} ms
    expected window: #{expected_window.message_count} messages in #{expected_window.duration_ms} ms (#{Float.round(expected_window.messages_per_second, 2)} msg/s)
    peak window:     #{peak_window.message_count} messages in #{peak_window.duration_ms} ms (#{Float.round(peak_window.messages_per_second, 2)} msg/s)
    result file: #{path}
    ==================================================================
    """)

    assert File.exists?(path)
    assert result.desired_consumers == desired_consumers
    assert result.expected_window.messages_per_second > 0
    assert result.peak_window.messages_per_second > 0
    assert resource_samples != []
    assert map_size(telemetry_tallies) > 0
  end

  defp run_expected_window(topics) do
    message_count = length(topics) * @messages_per_topic
    received_before = LoadRegexConsumer.received()
    window_start = System.monotonic_time(:millisecond)

    for topic <- topics, i <- 1..@messages_per_topic do
      assert {:ok, _} = PulsarEx.Cluster.produce("load", topic, "expected #{i}")
    end

    window_done =
      LoadTiming.wait_until(@window_timeout, fn ->
        LoadRegexConsumer.received() - received_before >= message_count
      end)

    duration_ms = window_done - window_start

    %{
      message_count: message_count,
      duration_ms: duration_ms,
      messages_per_second: message_count / (max(duration_ms, 1) / 1000)
    }
  end

  defp run_peak_window(topics) do
    message_count = length(topics) * @peak_messages_per_topic
    received_before = LoadRegexConsumer.received()
    window_start = System.monotonic_time(:millisecond)

    topics
    |> Task.async_stream(
      fn topic ->
        for i <- 1..@peak_messages_per_topic do
          assert {:ok, _} = PulsarEx.Cluster.produce("load", topic, "peak #{i}")
        end
      end,
      max_concurrency: @peak_max_concurrency,
      timeout: :infinity
    )
    |> Stream.run()

    window_done =
      LoadTiming.wait_until(@window_timeout, fn ->
        LoadRegexConsumer.received() - received_before >= message_count
      end)

    duration_ms = window_done - window_start

    %{
      message_count: message_count,
      duration_ms: duration_ms,
      messages_per_second: message_count / (max(duration_ms, 1) / 1000)
    }
  end

  defp read_broker_stats_summary(topics) do
    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    {stats_list, failed_count} =
      Enum.reduce(topics, {[], 0}, fn topic, {acc, failed} ->
        case LoadAdmin.partitioned_topic_stats(brokers, admin_port, topic) do
          {:ok, stats} -> {[stats | acc], failed}
          {:error, _reason} -> {acc, failed + 1}
        end
      end)

    %{
      msg_backlog_total: Enum.sum(Enum.map(stats_list, & &1.msg_backlog)),
      msg_rate_in_total: Enum.sum(Enum.map(stats_list, & &1.msg_rate_in)),
      msg_rate_out_total: Enum.sum(Enum.map(stats_list, & &1.msg_rate_out)),
      unacked_total:
        Enum.sum(
          Enum.map(stats_list, fn s -> Enum.sum(Map.values(s.unacked_by_subscription)) end)
        ),
      topics_failed: failed_count
    }
  end
end
