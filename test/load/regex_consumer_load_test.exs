defmodule PulsarEx.RegexConsumerLoadTest do
  use ExUnit.Case

  alias PulsarEx.{LoadTopics, LoadRegexConsumer, LoadTiming, TestAdmin}

  @topic_prefix "LoadSmoke"
  @topic_count 5
  @partition_count 3
  @messages_per_topic 20
  @topic_regex ~r/^LoadSmoke\d+$/
  @active_timeout 30_000
  @ready_timeout 30_000
  @drain_timeout 30_000

  @num_consumers 1

  setup do
    topics = LoadTopics.provision(@topic_prefix, @topic_count, @partition_count)

    for topic <- topics do
      TestAdmin.reset_subscription(topic, LoadRegexConsumer.subscription())
    end

    LoadRegexConsumer.reset(@topic_regex)

    on_exit(fn -> LoadRegexConsumer.stop(@topic_regex) end)

    {:ok, topics: topics}
  end

  @tag timeout: :infinity
  test "regex consumer discovers provisioned load topics and drains produced messages", %{
    topics: topics
  } do
    total_messages = @topic_count * @messages_per_topic
    desired_consumers = @topic_count * @partition_count * @num_consumers

    for topic <- topics, i <- 1..@messages_per_topic do
      assert {:ok, _} = PulsarEx.Cluster.produce("load", topic, "message #{i}")
    end

    {result, call_start, call_done} =
      LoadTiming.time_call(fn ->
        LoadRegexConsumer.start(@topic_regex, num_consumers: @num_consumers)
      end)

    assert :ok = result

    active_done = LoadRegexConsumer.wait_until_active(desired_consumers, @active_timeout)
    ready_done = LoadRegexConsumer.wait_until_ready(desired_consumers, @ready_timeout)

    drain_done =
      LoadTiming.wait_until(@drain_timeout, fn ->
        LoadRegexConsumer.received() >= total_messages
      end)

    IO.puts("""

    === Load test result: #{@topic_count} topics x #{@partition_count} partitions, num_consumers: #{@num_consumers} ===
    start_consumer call:                 #{call_done - call_start} ms
    time to #{desired_consumers} active consumers:     #{active_done - call_start} ms
    time to #{desired_consumers} ready consumers:      #{ready_done - call_start} ms
    time to drain #{total_messages} messages:          #{drain_done - call_start} ms
    ==================================================================
    """)

    assert LoadRegexConsumer.received() == total_messages
  end
end
