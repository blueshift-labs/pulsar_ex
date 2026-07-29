defmodule PulsarEx.RegexConsumerSteppedLoadTest do
  use ExUnit.Case

  alias PulsarEx.{LoadTopics, LoadRegexConsumer, LoadTiming, TestAdmin}

  @partition_count 5
  @messages_per_topic 5
  @active_timeout 60_000
  @ready_timeout 60_000
  @drain_timeout 60_000

  @num_consumers 1

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

  # Each stage gets its own topic prefix (not just an offset) so their
  # discovery regexes never match each other's topics — a shared regex
  # would let a smaller stage's consumer discover a larger stage's topics
  # too, and receive redelivered, previously-unacked messages from them.
  defp run_stage(topic_count) do
    topic_prefix = "LoadStep#{topic_count}_"
    topic_regex = Regex.compile!("^#{topic_prefix}\\d+$")

    on_exit(fn -> LoadRegexConsumer.stop(topic_regex) end)

    # Repeated start is a true no-op (Phase 3) — reset first in case a
    # previous, failed run of this same stage left a consumer running.
    LoadRegexConsumer.reset(topic_regex)

    topics = LoadTopics.provision(topic_prefix, topic_count, @partition_count)

    for topic <- topics do
      TestAdmin.reset_subscription(topic, LoadRegexConsumer.subscription())
    end

    total_messages = topic_count * @messages_per_topic
    desired_consumers = topic_count * @partition_count * @num_consumers

    for topic <- topics, i <- 1..@messages_per_topic do
      assert {:ok, _} = PulsarEx.Cluster.produce("load", topic, "message #{i}")
    end

    {result, call_start, call_done} =
      LoadTiming.time_call(fn ->
        LoadRegexConsumer.start(topic_regex, num_consumers: @num_consumers)
      end)

    assert :ok = result

    active_done = LoadRegexConsumer.wait_until_active(desired_consumers, @active_timeout)
    ready_done = LoadRegexConsumer.wait_until_ready(desired_consumers, @ready_timeout)

    drain_done =
      LoadTiming.wait_until(@drain_timeout, fn ->
        LoadRegexConsumer.received() >= total_messages
      end)

    IO.puts("""

    === Load stage result: #{topic_count} topics x #{@partition_count} partitions, num_consumers: #{@num_consumers} ===
    start_consumer call:             #{call_done - call_start} ms
    time to #{desired_consumers} active consumers: #{active_done - call_start} ms
    time to #{desired_consumers} ready consumers: #{ready_done - call_start} ms
    time to drain #{total_messages} messages:    #{drain_done - call_start} ms
    ==================================================================
    """)

    assert LoadRegexConsumer.received() == total_messages
  end
end
