defmodule PulsarEx.PartitionedTopicStatsTest do
  use ExUnit.Case

  alias PulsarEx.{Admin, TestAdmin}

  @topic "persistent://pulsar_ex/IntegrationTest/PartitionedTopicStatsTest"
  @subscription "partitioned_topic_stats_test"

  setup do
    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    # Ensure the durable subscription exists (first-ever run), then catch it
    # up to the current tail so leftover backlog from a prior test run
    # doesn't leak into this test's assertion.
    :ok = ensure_subscription(brokers, admin_port, @topic, @subscription)
    TestAdmin.reset_subscription(@topic, @subscription)

    {:ok, brokers: brokers, admin_port: admin_port}
  end

  @tag timeout: :infinity
  test "partitioned_topic_stats reflects the unconsumed backlog", %{
    brokers: brokers,
    admin_port: admin_port
  } do
    message_count = 25

    for i <- 1..message_count do
      assert {:ok, _} = PulsarEx.Cluster.produce("integration", @topic, "message #{i}")
    end

    Process.sleep(5_000)

    assert {:ok, stats} = Admin.partitioned_topic_stats(brokers, admin_port, @topic)

    # No consumer ever attaches in this test, so every produced message sits
    # in the backlog, and nothing is "unacked" since nothing was delivered.
    assert stats.msg_backlog == message_count
    assert Map.get(stats.unacked_by_subscription, @subscription, 0) == 0
    assert is_number(stats.msg_rate_in)
    assert is_number(stats.msg_rate_out)
  end

  defp ensure_subscription(hosts, admin_port, topic, subscription) when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case ensure_subscription(host, admin_port, topic, subscription) do
        :ok -> {:halt, :ok}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end

  defp ensure_subscription(host, admin_port, topic, subscription) when is_binary(host) do
    path = String.replace(topic, "://", "/")

    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/#{path}/subscription/#{subscription}"
    }

    with {:ok, status, _, client_ref} when status in [204, 409] <-
           :hackney.put(
             URI.to_string(url),
             [{"Content-Type", "application/json"}],
             "\"earliest\"",
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
