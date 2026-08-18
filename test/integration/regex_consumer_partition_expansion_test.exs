defmodule PulsarEx.RegexConsumerPartitionExpansionTest do
  use ExUnit.Case

  alias PulsarEx.{TestRegexConsumer, TestAdmin, Admin, Partitioner, Topic}

  @topic "persistent://pulsar_ex/IntegrationTest/RegexPartitionExpansionTest"
  @topic_regex ~r/^RegexPartitionExpansionTest$/
  @messages_per_partition 3
  @added_partitions 2

  setup do
    Application.put_env(:pulsar_ex, :partition_watch_interval, 2_000)

    on_exit(fn ->
      Application.delete_env(:pulsar_ex, :partition_watch_interval)
    end)

    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()
    TestAdmin.reset_subscription(@topic, TestRegexConsumer.subscription())

    {:ok, []}
  end

  @tag timeout: :infinity
  test "regex consumer detects partition expansion and consumes the new partitions without a restart" do
    brokers = Application.get_env(:pulsar_ex, :brokers)
    admin_port = Application.get_env(:pulsar_ex, :admin_port)

    {:ok, %Topic{} = topic} = Topic.parse(@topic)
    # Query the live partition count directly rather than assuming the topic
    # still has the 3 partitions from config/integration.exs: Pulsar partition
    # counts can only grow, never shrink, so a prior run of this same test
    # against this long-lived broker may have already expanded it.
    {:ok, initial_partitions} = Admin.lookup_topic_partitions(brokers, admin_port, topic)

    produce_to_partitions(initial_partitions, 0..(initial_partitions - 1), "initial")

    assert :ok = TestRegexConsumer.start(@topic_regex, refresh_interval: 3_000)

    Process.sleep(10_000)

    assert [received: initial_total] = TestRegexConsumer.received()
    assert initial_total == initial_partitions * @messages_per_partition

    expanded_partitions = initial_partitions + @added_partitions
    assert :ok = Admin.update_partitioned_topic(brokers, admin_port, @topic, expanded_partitions)

    # Give the (test-shortened) partition watch time to notice the new count,
    # then the consumer's own refresh time to start consumers for it.
    Process.sleep(20_000)

    produce_to_partitions(
      expanded_partitions,
      initial_partitions..(expanded_partitions - 1),
      "expanded"
    )

    Process.sleep(10_000)

    assert [received: total] = TestRegexConsumer.received()
    assert total == initial_total + @added_partitions * @messages_per_partition
  end

  defp produce_to_partitions(total_partitions, target_partitions, label) do
    for {partition, key} <- keys_by_partition(total_partitions, target_partitions) do
      for i <- 1..@messages_per_partition do
        assert {:ok, _} =
                 PulsarEx.Cluster.produce(
                   "integration",
                   @topic,
                   "#{label} partition #{partition} message #{i}",
                   partition_key: key
                 )
      end
    end
  end

  defp keys_by_partition(total_partitions, target_partitions) do
    targets = MapSet.new(target_partitions)

    0
    |> Stream.iterate(&(&1 + 1))
    |> Stream.map(&"partition_key_#{&1}")
    |> Enum.reduce_while(%{}, fn key, acc ->
      partition = Partitioner.assign(key, total_partitions)

      acc =
        if MapSet.member?(targets, partition) do
          Map.put_new(acc, partition, key)
        else
          acc
        end

      if map_size(acc) == MapSet.size(targets) do
        {:halt, acc}
      else
        {:cont, acc}
      end
    end)
  end
end
