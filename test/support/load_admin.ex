defmodule PulsarEx.LoadAdmin do
  @moduledoc """
  Load-scoped wrapper around `PulsarEx.Admin` that times every call and tallies
  count / min / max / avg latency per function name into an ETS table, so load
  scenarios can report Admin request rate/latency without touching the real
  Admin module. Delegates every call unmodified - return values are identical
  to calling `PulsarEx.Admin` directly.

  Ownership contract: the stats table is owned by whichever process first
  calls `reset/0` (or any wrapped function, which lazily creates the table).
  Call `reset/0` once from the long-lived scenario process *before* spawning
  any workers - if an ephemeral worker process creates the table instead, the
  table (and every tally in it) dies with that worker.
  """

  alias PulsarEx.Admin

  @table :load_admin_call_stats

  def reset() do
    ensure_table()
    :ets.delete_all_objects(@table)
  end

  def stats() do
    ensure_table()

    @table
    |> :ets.tab2list()
    |> Map.new(fn {name, count, sum_us, min_us, max_us} ->
      avg_us = if count > 0, do: sum_us / count, else: 0.0

      {name,
       %{
         count: count,
         sum_us: sum_us,
         min_us: min_us,
         max_us: max_us,
         avg_us: avg_us
       }}
    end)
  end

  def lookup_topic_partitions(hosts, admin_port, topic) do
    call(:lookup_topic_partitions, [hosts, admin_port, topic])
  end

  def lookup_topic(hosts, admin_port, topic) do
    call(:lookup_topic, [hosts, admin_port, topic])
  end

  def discover_topics(hosts, admin_port, tenant, namespace, topic_name_or_regex) do
    call(:discover_topics, [hosts, admin_port, tenant, namespace, topic_name_or_regex])
  end

  def discover_clusters(hosts, admin_port) do
    call(:discover_clusters, [hosts, admin_port])
  end

  def create_tenant(hosts, admin_port, tenant, clusters) do
    call(:create_tenant, [hosts, admin_port, tenant, clusters])
  end

  def create_namespace(hosts, admin_port, namespace, policies \\ %{}) do
    call(:create_namespace, [hosts, admin_port, namespace, policies])
  end

  def create_topic(hosts, admin_port, topic) do
    call(:create_topic, [hosts, admin_port, topic])
  end

  def create_partitioned_topic(hosts, admin_port, topic, partitions \\ 1) do
    call(:create_partitioned_topic, [hosts, admin_port, topic, partitions])
  end

  def update_partitioned_topic(hosts, admin_port, topic, partitions) do
    call(:update_partitioned_topic, [hosts, admin_port, topic, partitions])
  end

  def delete_partitioned_topic(hosts, admin_port, topic, force \\ false) do
    call(:delete_partitioned_topic, [hosts, admin_port, topic, force])
  end

  def partitioned_topic_stats(hosts, admin_port, topic) do
    call(:partitioned_topic_stats, [hosts, admin_port, topic])
  end

  defp call(name, args) do
    ensure_table()

    start = System.monotonic_time(:microsecond)
    result = apply(Admin, name, args)
    elapsed_us = System.monotonic_time(:microsecond) - start

    tally(name, elapsed_us)

    result
  end

  defp tally(name, elapsed_us) do
    :ets.update_counter(
      @table,
      name,
      [{2, 1}, {3, elapsed_us}],
      {name, 0, 0, elapsed_us, elapsed_us}
    )

    case :ets.lookup(@table, name) do
      [{^name, _count, _sum_us, min_us, max_us}] ->
        if elapsed_us < min_us, do: :ets.update_element(@table, name, {4, elapsed_us})
        if elapsed_us > max_us, do: :ets.update_element(@table, name, {5, elapsed_us})

      [] ->
        :ok
    end
  end

  defp ensure_table() do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])
    end
  end
end
