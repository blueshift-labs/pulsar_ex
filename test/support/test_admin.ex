defmodule PulsarEx.TestAdmin do
  @moduledoc false

  alias PulsarEx.Admin

  @table :test_admin_overrides
  @count_table :test_admin_call_counts

  def override(tenant, namespace, topic_name_or_regex, response) do
    ensure_table()
    :ets.insert(@table, {{tenant, namespace, topic_name_or_regex}, response})
  end

  def clear_overrides() do
    ensure_table()
    :ets.delete_all_objects(@table)
  end

  @doc "Resets the per-key call counter used by `call_count/3`."
  def reset_counts() do
    ensure_count_table()
    :ets.delete_all_objects(@count_table)
  end

  @doc "Number of `discover_topics/5` calls observed for this key since the last `reset_counts/0`."
  def call_count(tenant, namespace, topic_name_or_regex) do
    ensure_count_table()

    case :ets.lookup(@count_table, {tenant, namespace, topic_name_or_regex}) do
      [{_key, count}] -> count
      [] -> 0
    end
  end

  def discover_topics(brokers, admin_port, tenant, namespace, topic_name_or_regex) do
    ensure_table()
    ensure_count_table()

    key = {tenant, namespace, topic_name_or_regex}
    :ets.update_counter(@count_table, key, {2, 1}, {key, 0})

    case :ets.lookup(@table, key) do
      [{_key, response}] -> response
      [] -> Admin.discover_topics(brokers, admin_port, tenant, namespace, topic_name_or_regex)
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

  defp ensure_count_table() do
    if :ets.whereis(@count_table) == :undefined do
      :ets.new(@count_table, [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])
    end
  end

  @doc """
  Marks all currently backlogged messages on a subscription as acknowledged, via
  Pulsar's `skip_all` admin endpoint, so a test starts from a clean cursor instead
  of replaying whatever an earlier test run left unacked. Confirmed against a live
  broker: calling this on a partitioned topic's base name fans out to every
  partition in one call. A subscription that doesn't exist yet (first-ever run)
  404s, treated as a no-op.
  """
  def reset_subscription(topic_name, subscription) do
    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    :ok = skip_all(brokers, admin_port, topic_name, subscription)
  end

  defp skip_all(hosts, admin_port, topic_name, subscription) when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case skip_all(host, admin_port, topic_name, subscription) do
        :ok -> {:halt, :ok}
        {:error, _} = err -> {:cont, err}
      end
    end)
  end

  defp skip_all(host, admin_port, topic_name, subscription) when is_binary(host) do
    path = String.replace(topic_name, "://", "/")

    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/#{path}/subscription/#{subscription}/skip_all"
    }

    with {:ok, status, _, client_ref} when status in [204, 404] <-
           :hackney.post(URI.to_string(url), [], "",
             follow_redirect: true,
             force_redirect: true
           ) do
      {:ok, _} = :hackney.body(client_ref)
      :ok
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end
end
