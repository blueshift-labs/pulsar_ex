defmodule PulsarEx.TestPartitionManager do
  @moduledoc false

  alias PulsarEx.PartitionManager

  @table :test_partition_manager_overrides

  def override(cluster_name, topic_name, response) do
    ensure_table()
    :ets.insert(@table, {{cluster_name, topic_name}, response})
  end

  def clear_overrides() do
    ensure_table()
    :ets.delete_all_objects(@table)
  end

  def lookup(cluster_name, topic_name) do
    ensure_table()

    case :ets.lookup(@table, {cluster_name, topic_name}) do
      [{_key, response}] -> response
      [] -> PartitionManager.lookup(cluster_name, topic_name)
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
