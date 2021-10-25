defmodule PulsarEx.Partitioner do
  def assign(_, 0), do: nil

  def assign(nil, partitions), do: :rand.uniform(partitions) - 1

  def assign(partition_key, partitions), do: :erlang.phash2(partition_key, partitions)
end
