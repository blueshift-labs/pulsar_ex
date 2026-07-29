defmodule PulsarEx.RegexConsumerRepeatedStartTest do
  use ExUnit.Case, async: false

  alias PulsarEx.{TestRegexConsumer, TestAdmin}

  @cluster "integration"
  @tenant "pulsar_ex"
  @namespace "IntegrationTest"

  # Anchored regex matches no real topic, so discover_topics always returns
  # {:ok, []} -- no consumer, subscription, or message is ever created. Only
  # the ConsumerManager's timer bookkeeping is exercised.
  @topic_regex ~r/^RegexTimerLeakTest$/
  @subscription "regex_consumer_timer_leak_test"
  @refresh_interval 3_000

  setup do
    Application.put_env(:pulsar_ex, :admin_module, TestAdmin)
    TestAdmin.clear_overrides()
    TestAdmin.reset_counts()

    on_exit(fn -> Application.delete_env(:pulsar_ex, :admin_module) end)

    {:ok, []}
  end

  # A repeated start for an already-running {cluster, tenant, namespace,
  # topic_regex, subscription} key is a true no-op: it returns :ok without
  # calling discover_topics or touching the existing timer, mirroring
  # do_start_consumer's existing {:error, {:already_started, _}} -> nil
  # handling one level down. So only the first start ever reaches
  # discover_topics (call count 1 right after both starts return), and only
  # the first start's timer is ever scheduled, so exactly one refresh fires
  # after one refresh_interval (call count 2 total).
  @tag timeout: :infinity
  test "repeated start without an intervening stop is a no-op and does not leak the previous refresh timer" do
    start_opts = [initial_position: :earliest, refresh_interval: @refresh_interval]

    assert :ok =
             PulsarEx.Cluster.start_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               @subscription,
               TestRegexConsumer,
               start_opts
             )

    assert :ok =
             PulsarEx.Cluster.start_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               @subscription,
               TestRegexConsumer,
               start_opts
             )

    assert TestAdmin.call_count(@tenant, @namespace, @topic_regex) == 1

    Process.sleep(@refresh_interval + 2_000)

    assert TestAdmin.call_count(@tenant, @namespace, @topic_regex) == 2
  end
end
