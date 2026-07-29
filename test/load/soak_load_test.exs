defmodule PulsarEx.SoakLoadTest do
  @moduledoc """
  Long-duration soak scenario (Phase 6, Task 6.E.1).

  Runs a regex consumer against a small, fixed topic/partition set under a
  steady (light but non-zero) produce rate for `@duration_ms`, sampling BEAM
  resources, telemetry, and broker-side backlog throughout, then asserts
  none of them show a sustained upward trend over the run.

  IMPORTANT: `@duration_ms` below is a short *mechanism-verification* smoke
  duration (2.5 minutes), not the real soak target. A real multi-hour soak
  (proposed default: 2 hours, at the "expected window" throughput already
  measured at production scale earlier in this load harness) is a deliberate
  separate step someone runs later by bumping this one constant (e.g. to
  `7_200_000` for 2 hours) or passing a different value - it is NOT implied
  by this test passing once at the smoke duration.
  """

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

  # --- Soak duration ---------------------------------------------------
  # Smoke default - see moduledoc. Bump this one constant for a real soak.
  @duration_ms 150_000

  # Modest, fixed footprint on purpose: this test is about *duration*
  # stability, not scale (Task 6.A/Phase 4 already covered scale up to
  # 1000x5 topics and 25,000 consumers).
  @topic_count 10
  @partition_count 5
  @num_consumers 1

  # No topic churn happens in this test (unlike Task 6.A's churn scenario),
  # so the default refresh_interval would be fine as-is; shortened anyway
  # only so discovery completes a little faster right at test start.
  @refresh_interval 5_000

  # ~5 msg/s spread round-robin over @topic_count topics (0.5 msg/s/topic) -
  # deliberately far below Phase 4's measured expected-window throughput
  # (145-225 msg/s at 100x5 scale, single consumer). This test is about
  # sustained *duration* stability, not peak throughput, so a light but
  # non-zero steady rate is enough to keep the consumer, its mailboxes, and
  # the broker continuously doing real work for the run's entire length.
  @produce_interval_ms 200

  # Resource sampler cadence: fine-grained enough to catch short-lived
  # spikes, coarse enough that even a multi-hour soak (2h / 2s = 3600
  # samples) stays a manageable series to store and inspect afterward.
  @sampler_interval_ms 2_000

  # Broker-side partitioned-topic-stats cadence, deliberately coarser than
  # the resource sampler: each tick is @topic_count real Admin HTTP round
  # trips, unlike the resource sampler's local, non-network reads. Polling
  # at the resource sampler's 2s cadence would mean 10 Admin calls every 2s
  # for the whole run (5 calls/s sustained just for instrumentation). 10s
  # keeps that at a light ~1 call/s while still giving enough samples to see
  # a trend (2h / 10s = 720 samples).
  @broker_stats_interval_ms 10_000

  @active_timeout 60_000
  @ready_timeout 60_000
  @settle_timeout 30_000

  # --- Stability tolerances --------------------------------------------
  # "No sustained upward trend" is checked as:
  #   mean(last quartile) <= mean(first quartile) * (1 + tolerance) + floor
  # Quartiles (not first-sample-vs-last-sample) so a one-off transient spike
  # near either edge of the run doesn't get mistaken for a trend, and a real
  # slow trend that happens to dip momentarily right at the end doesn't get
  # missed. The additive absolute floor exists because a pure percentage
  # bound is meaningless (or too tight) against a near-zero baseline (e.g.
  # mailbox lengths, expected to sit at ~0 under this light steady load) -
  # it also absorbs ordinary jitter (GC timing, binary refcounts) that a
  # real multi-hour leak would dwarf.
  @process_count_tolerance_fraction 0.15
  @process_count_absolute_floor 20

  @memory_tolerance_fraction 0.25
  @memory_absolute_floor_bytes 5_000_000

  @mailbox_tolerance_fraction 0.10
  @mailbox_absolute_floor 5

  # Broker backlog: with a single consumer continuously draining a light
  # (0.5 msg/s/topic) steady rate, backlog is expected to sit near zero
  # throughout, with only the small transient blips Phase 4 already observed
  # from batched acking (~1s ack_interval). A flat absolute cap checked on
  # *every* sample (not just a quartile average) is a stronger and simpler
  # "stays bounded throughout, not just at the end" check than a trend
  # comparison would be here, since near-zero-by-design doesn't need a
  # trend comparison to prove it stayed near zero.
  @broker_backlog_cap 200

  @moduletag timeout: :infinity

  test "soak: steady load over #{@duration_ms}ms shows no resource or backlog trend" do
    prefix = "Soak#{System.unique_integer([:positive])}_"
    topic_regex = Regex.compile!("^#{prefix}\\d+$")

    on_exit(fn -> LoadRegexConsumer.stop(topic_regex) end)

    LoadRegexConsumer.reset(topic_regex)

    LoadAdmin.reset()

    topics = LoadTopics.provision(prefix, @topic_count, @partition_count, 0, LoadAdmin)

    for topic <- topics do
      TestAdmin.reset_subscription(topic, LoadRegexConsumer.subscription())
    end

    # Captured before starting anything - `LoadTiming.wait_until*` return
    # absolute monotonic timestamps, not durations, so every elapsed value
    # below is computed by subtracting this.
    run_start = System.monotonic_time(:millisecond)

    {:ok, sampler_pid} = LoadResourceSampler.start(@sampler_interval_ms)
    telemetry_handle = LoadTelemetryCounter.start()

    desired_consumers = @topic_count * @partition_count * @num_consumers

    assert :ok =
             LoadRegexConsumer.start(topic_regex,
               num_consumers: @num_consumers,
               refresh_interval: @refresh_interval
             )

    active_done = LoadRegexConsumer.wait_until_active(desired_consumers, @active_timeout)
    ready_done = LoadRegexConsumer.wait_until_ready(desired_consumers, @ready_timeout)

    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    received_before = LoadRegexConsumer.received()
    producer_start = System.monotonic_time(:millisecond)
    producer_deadline = producer_start + @duration_ms

    producer_result =
      run_producer_loop(
        topics,
        run_start,
        producer_deadline,
        brokers,
        admin_port,
        %{index: 0, produced: 0, next_broker_sample_at: producer_start, broker_samples: []}
      )

    window_done =
      LoadTiming.wait_until(@settle_timeout, fn ->
        LoadRegexConsumer.received() - received_before >= producer_result.produced
      end)

    resource_samples = LoadResourceSampler.stop(sampler_pid)
    telemetry_tallies = LoadTelemetryCounter.stop(telemetry_handle)
    admin_stats = LoadAdmin.stats()

    LoadRegexConsumer.stop(topic_regex)

    broker_samples = Enum.reverse(producer_result.broker_samples)

    assert resource_samples != []

    assert length(resource_samples) >= 8,
           "not enough resource samples (#{length(resource_samples)}) for a meaningful " <>
             "quartile trend comparison - increase @duration_ms or decrease @sampler_interval_ms"

    assert broker_samples != []

    assert length(broker_samples) >= 4,
           "not enough broker-stats samples (#{length(broker_samples)}) for a meaningful " <>
             "trend comparison - increase @duration_ms or decrease @broker_stats_interval_ms"

    # --- Stability assertions -----------------------------------------
    process_count_trend =
      assert_no_upward_trend(
        "process_count",
        Enum.map(resource_samples, & &1.process_count),
        @process_count_tolerance_fraction,
        @process_count_absolute_floor
      )

    memory_fields = resource_samples |> hd() |> Map.fetch!(:memory) |> Keyword.keys()

    memory_trends =
      Map.new(memory_fields, fn field ->
        trend =
          assert_no_upward_trend(
            "memory.#{field}",
            Enum.map(resource_samples, &Keyword.fetch!(&1.memory, field)),
            @memory_tolerance_fraction,
            @memory_absolute_floor_bytes
          )

        {field, trend}
      end)

    mailbox_trend =
      assert_no_upward_trend(
        "mailbox_lengths",
        Enum.map(resource_samples, fn s -> sum_mailbox(s.mailbox_lengths) end),
        @mailbox_tolerance_fraction,
        @mailbox_absolute_floor
      )

    supervisor_mailbox_trend =
      assert_no_upward_trend(
        "supervisor_mailbox_lengths",
        Enum.map(resource_samples, fn s -> sum_mailbox(s.supervisor_mailbox_lengths) end),
        @mailbox_tolerance_fraction,
        @mailbox_absolute_floor
      )

    # Broker backlog stays bounded throughout the run, not just at the end -
    # checked on every sample (see comment on @broker_backlog_cap above).
    Enum.each(broker_samples, fn sample ->
      assert sample.msg_backlog_total <= @broker_backlog_cap,
             "broker backlog #{sample.msg_backlog_total} at elapsed_ms=#{sample.elapsed_ms} " <>
               "exceeded cap #{@broker_backlog_cap}"
    end)

    # Discovery must never error over the course of the soak - a real
    # correctness signal, cheap to check since the telemetry counter is
    # already running for the whole soak anyway.
    discovery_error_count =
      get_in(telemetry_tallies, [[:pulsar_ex, :consumer_manager, :discovery, :error], :count]) ||
        0

    assert discovery_error_count == 0

    result = %{
      duration_ms: @duration_ms,
      topic_count: @topic_count,
      partition_count: @partition_count,
      num_consumers: @num_consumers,
      produce_interval_ms: @produce_interval_ms,
      sampler_interval_ms: @sampler_interval_ms,
      broker_stats_interval_ms: @broker_stats_interval_ms,
      desired_consumers: desired_consumers,
      messages_produced: producer_result.produced,
      timing: %{
        time_to_active_ms: active_done - run_start,
        time_to_ready_ms: ready_done - run_start,
        time_to_settled_ms: window_done - run_start
      },
      stability_analysis: %{
        process_count: process_count_trend,
        memory: memory_trends,
        mailbox_lengths: mailbox_trend,
        supervisor_mailbox_lengths: supervisor_mailbox_trend
      },
      broker_backlog_cap: @broker_backlog_cap,
      resource_samples: LoadResourceSampler.json_safe(resource_samples, run_start),
      broker_samples: broker_samples,
      telemetry_tallies: LoadTelemetryCounter.json_safe(telemetry_tallies),
      admin_stats: admin_stats
    }

    File.mkdir_p!("test/load/results")
    timestamp = System.system_time(:second)
    path = "test/load/results/soak-#{timestamp}.json"
    File.write!(path, Jason.encode!(result, pretty: true))

    IO.puts("""

    === Soak result: #{@topic_count} topics x #{@partition_count} partitions, #{@duration_ms}ms ===
    messages produced:  #{producer_result.produced}
    time to active:     #{active_done - run_start} ms
    time to ready:       #{ready_done - run_start} ms
    time to settled:     #{window_done - run_start} ms
    resource samples:    #{length(resource_samples)}
    broker samples:      #{length(broker_samples)}
    result file: #{path}
    ==================================================================
    """)

    assert File.exists?(path)
  end

  # Produces one message at a time, round-robin over `topics`, sleeping
  # `@produce_interval_ms` between each, until `deadline` (an absolute
  # monotonic timestamp) is reached. Along the way, takes a broker-stats
  # snapshot across all topics every `@broker_stats_interval_ms` - this
  # briefly pauses production for however long @topic_count sequential Admin
  # HTTP calls take (each snapshot tick is inline in this same loop, not on
  # its own process), so the produce rate isn't perfectly uniform. Acceptable
  # here since this soak's premise is sustained *duration*, not precise
  # rate - a sub-second gap every @broker_stats_interval_ms doesn't change
  # what's being verified. This is a plain tail-recursive loop (the recursive
  # call is the function's final expression) so it doesn't grow the stack
  # across however many iterations a multi-hour run needs.
  defp run_producer_loop(topics, run_start, deadline, brokers, admin_port, acc) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline do
      acc
    else
      topic = Enum.at(topics, rem(acc.index, length(topics)))

      assert {:ok, _} =
               PulsarEx.Cluster.produce(LoadRegexConsumer.cluster(), topic, "soak #{acc.index}")

      acc =
        if now >= acc.next_broker_sample_at do
          sample = sample_broker_stats(topics, brokers, admin_port, now - run_start)

          %{
            acc
            | broker_samples: [sample | acc.broker_samples],
              next_broker_sample_at: now + @broker_stats_interval_ms
          }
        else
          acc
        end

      Process.sleep(@produce_interval_ms)

      run_producer_loop(topics, run_start, deadline, brokers, admin_port, %{
        acc
        | index: acc.index + 1,
          produced: acc.produced + 1
      })
    end
  end

  defp sample_broker_stats(topics, brokers, admin_port, elapsed_ms) do
    {stats_list, failed_count} =
      Enum.reduce(topics, {[], 0}, fn topic, {acc, failed} ->
        case LoadAdmin.partitioned_topic_stats(brokers, admin_port, topic) do
          {:ok, stats} -> {[stats | acc], failed}
          {:error, _reason} -> {acc, failed + 1}
        end
      end)

    %{
      elapsed_ms: elapsed_ms,
      msg_backlog_total: Enum.sum(Enum.map(stats_list, & &1.msg_backlog)),
      msg_rate_in_total: Enum.sum(Enum.map(stats_list, & &1.msg_rate_in)),
      msg_rate_out_total: Enum.sum(Enum.map(stats_list, & &1.msg_rate_out)),
      topics_failed: failed_count
    }
  end

  defp sum_mailbox(mailbox_lengths),
    do: Enum.sum(Enum.map(mailbox_lengths, fn {_pid, len} -> len end))

  # Compares mean(last quartile) against mean(first quartile) - see the
  # tolerance comment block above for why quartiles + a percentage-plus-
  # absolute-floor bound, rather than first-vs-last-sample or a pure
  # percentage.
  defp assert_no_upward_trend(label, values, tolerance_fraction, absolute_floor) do
    n = length(values)
    q = max(1, div(n, 4))
    first = Enum.take(values, q)
    last = Enum.take(values, -q)
    first_mean = Enum.sum(first) / length(first)
    last_mean = Enum.sum(last) / length(last)
    bound = first_mean * (1 + tolerance_fraction) + absolute_floor

    assert last_mean <= bound,
           "#{label}: last-quartile mean #{last_mean} exceeded bound #{bound} " <>
             "(first-quartile mean #{first_mean}, tolerance #{tolerance_fraction * 100}%, " <>
             "floor #{absolute_floor})"

    %{first_quartile_mean: first_mean, last_quartile_mean: last_mean, bound: bound}
  end
end
