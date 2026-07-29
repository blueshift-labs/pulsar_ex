defmodule PulsarEx.LoadTopics do
  @moduledoc false

  alias PulsarEx.{Admin, Topic}

  @tenant "pulsar_ex"
  @namespace "Load"

  def tenant(), do: @tenant
  def namespace(), do: @namespace

  def topic_name(prefix, idx), do: "#{prefix}#{idx}"

  def topic_url(prefix, idx),
    do: "persistent://#{@tenant}/#{@namespace}/#{topic_name(prefix, idx)}"

  @doc """
  Idempotently ensures `count` topics named `<prefix><idx>` (indices starting
  at `offset`) exist, each with at least `partitions` partitions. Safe to call
  repeatedly (e.g. with a larger `count`, or a larger `partitions` than an
  earlier run used) against the same long-lived broker — existing topics with
  fewer partitions are grown via `update_partitioned_topic` (partition counts
  only grow, never shrink), never recreated.

  Give each distinct load-test scenario its own `prefix`, and each stage
  within a scenario its own `offset`, so their topic-index ranges never
  overlap. A shared regex discovers every topic matching its pattern
  regardless of which stage's `provision/4` call created it — an overlapping
  range means a smaller stage's consumer also discovers (and can receive
  redelivered, previously-unacked messages from) a larger stage's topics.
  """
  def provision(prefix, count, partitions, offset \\ 0, admin_module \\ Admin) do
    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    offset..(offset + count - 1)
    |> Enum.map(fn idx ->
      topic = topic_url(prefix, idx)
      :ok = admin_module.create_partitioned_topic(brokers, admin_port, topic, partitions)

      {:ok, parsed} = Topic.parse(topic)

      {:ok, current_partitions} =
        admin_module.lookup_topic_partitions(brokers, admin_port, parsed)

      if current_partitions < partitions do
        :ok = admin_module.update_partitioned_topic(brokers, admin_port, topic, partitions)
      end

      topic
    end)
  end

  @doc """
  Idempotently deletes `count` topics named `<prefix><idx>` (indices starting
  at `offset`). Safe to call for topics that don't exist yet or were already
  deleted by an earlier call — `Admin.delete_partitioned_topic/4` treats a 404
  ("already gone") as success, the same way `provision/4` treats a 409
  ("already exists") as success.
  """
  def delete(prefix, count, offset \\ 0, admin_module \\ Admin) do
    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    offset..(offset + count - 1)
    |> Enum.each(fn idx ->
      topic = topic_url(prefix, idx)
      :ok = admin_module.delete_partitioned_topic(brokers, admin_port, topic)
    end)
  end
end
