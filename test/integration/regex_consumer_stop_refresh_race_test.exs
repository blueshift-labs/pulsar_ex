defmodule PulsarEx.RegexConsumerStopRefreshRaceTest do
  use ExUnit.Case, async: false

  alias PulsarEx.{TestRegexConsumer, TestAdmin}

  @cluster "integration"
  @tenant "pulsar_ex"
  @namespace "IntegrationTest"

  # Anchored regex matches no real topic, so discover_topics always returns
  # {:ok, []} -- no consumer, subscription, or message is ever created. Only
  # ConsumerManager's timer/handle_info bookkeeping is exercised.
  @topic_regex ~r/^RegexStopRefreshRaceTest$/
  @subscription "regex_consumer_stop_refresh_race_test"
  @refresh_interval 60_000

  setup do
    Application.put_env(:pulsar_ex, :admin_module, TestAdmin)
    TestAdmin.clear_overrides()
    TestAdmin.reset_counts()

    on_exit(fn ->
      PulsarEx.Cluster.stop_consumer(@cluster, @tenant, @namespace, @topic_regex, @subscription)
      Application.delete_env(:pulsar_ex, :admin_module)
    end)

    {:ok, []}
  end

  # Process.cancel_timer/1 can only prevent a future firing, not retract a
  # :refresh message already sitting in ConsumerManager's mailbox. This test
  # simulates that already-in-flight message directly (via :sys.get_state to
  # grab the real Cluster struct, then a raw send/2 shaped exactly like the
  # production :refresh message) rather than racing a real timer, since the
  # actual race window is microseconds wide and not reproducible by sleeping.
  # The stale message carries a placeholder refresh_ref (make_ref/0) rather
  # than the real one that was assigned at start: after stop pops the pattern
  # out of `timers` entirely, handle_info's Map.fetch on the key fails
  # regardless of the ref value, so any ref proves the point -- a message
  # from a genuinely still-running pattern with a *mismatched* ref (e.g. from
  # a stop immediately followed by a restart) is a related but distinct
  # scenario this test does not cover.
  @tag timeout: :infinity
  test "a stale refresh message delivered after stop does not resurrect the stopped pattern" do
    consumer_opts = [initial_position: :earliest, refresh_interval: @refresh_interval]

    assert :ok =
             PulsarEx.Cluster.start_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               @subscription,
               TestRegexConsumer,
               consumer_opts
             )

    assert :ok =
             PulsarEx.Cluster.stop_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               @subscription
             )

    # 1 discover_topics call from start's do_start_consumers. Stop no longer
    # discovers at all -- it stops consumers from the pattern's stored
    # matched-topic set (empty here, since this regex matches no real topic).
    assert TestAdmin.call_count(@tenant, @namespace, @topic_regex) == 1

    manager = Process.whereis(PulsarEx.ConsumerManager)
    %{clusters: clusters} = :sys.get_state(manager)
    cluster = Map.fetch!(clusters, @cluster)

    send(
      manager,
      {:refresh, cluster, @tenant, @namespace, @topic_regex, @subscription, TestRegexConsumer,
       consumer_opts, @refresh_interval, make_ref()}
    )

    Process.sleep(200)

    # Stays at 1: handle_info looks up the pattern's key in `timers`, finds it
    # absent (stop already popped it), and drops the stale message instead of
    # calling do_start_consumers again.
    assert TestAdmin.call_count(@tenant, @namespace, @topic_regex) == 1

    %{timers: timers} = :sys.get_state(manager)
    refute Map.has_key?(timers, {@cluster, @tenant, @namespace, @topic_regex, @subscription})
  end
end
