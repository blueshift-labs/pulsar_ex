defmodule PulsarEx.RegexConsumerPartitionFanoutTest do
  use ExUnit.Case

  alias PulsarEx.{TestRegexConsumer, TestAdmin, Partitioner}

  @topic "persistent://pulsar_ex/IntegrationTest/RegexPartitionFanoutTest"
  @topic_regex ~r/^RegexPartitionFanoutTest$/
  @partitions 5
  @messages_per_partition 3

  setup do
    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()
    TestAdmin.reset_subscription(@topic, TestRegexConsumer.subscription())

    {:ok, []}
  end

  @tag timeout: :infinity
  test "regex consumer starts one consumer per partition and receives messages from every partition" do
    partition_keys = keys_by_partition(@partitions)

    for partition <- 0..(@partitions - 1) do
      key = Map.fetch!(partition_keys, partition)

      for i <- 1..@messages_per_partition do
        assert {:ok, _} =
                 PulsarEx.Cluster.produce(
                   "integration",
                   @topic,
                   "partition #{partition} message #{i}",
                   partition_key: key
                 )
      end
    end

    assert :ok = TestRegexConsumer.start(@topic_regex)

    Process.sleep(10_000)

    assert [received: total] = TestRegexConsumer.received()
    assert total == @partitions * @messages_per_partition
  end

  defp keys_by_partition(partitions) do
    0
    |> Stream.iterate(&(&1 + 1))
    |> Stream.map(&"partition_key_#{&1}")
    |> Enum.reduce_while(%{}, fn key, acc ->
      partition = Partitioner.assign(key, partitions)
      acc = Map.put_new(acc, partition, key)

      if map_size(acc) == partitions do
        {:halt, acc}
      else
        {:cont, acc}
      end
    end)
  end
end
