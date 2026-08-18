defmodule PulsarEx.ConsumerManagerRestartLoadTest do
  @moduledoc """
  Fault-injection coverage for two different kinds of "the process holding
  discovery state goes away" event: a bare `ConsumerManager` crash (caught by
  `PulsarEx.Supervisor`'s `rest_for_one` strategy) and a full `:pulsar_ex`
  application stop/start.

  These two faults are NOT equivalent, and this file asserts that on purpose:

  - `ConsumerManager` is the last child in `PulsarEx.Application.children/1`,
    so under `rest_for_one` only `ConsumerManager` itself gets restarted when
    it crashes - nothing earlier in the child list (in particular
    `ConsumerSupervisor`, which actually owns the running per-partition
    consumer processes) is touched. A `ConsumerManager` crash therefore loses
    only its own in-memory bookkeeping (`timers`, desired-consumer snapshots);
    already-running consumers keep serving uninterrupted.
  - `Application.stop(:pulsar_ex)` followed by `Application.start(:pulsar_ex)`
    tears down and rebuilds every registry, supervisor, and consumer process
    in the application - a genuine full reset, not just the manager's
    bookkeeping.

  `ConsumerManager`'s own GenServer state (`timers` and desired-consumer
  snapshots, read via `ConsumerManager.desired_consumers/4`) is in-memory only
  and is expected to be lost on both faults. Proof of "it needed a fresh
  `start_consumer` call to be rebuilt from scratch" is a `0 -> desired`
  transition after calling `start` again post-fault, not persistence.

  Verification note: this file is expected to pass standalone
  (`MIX_ENV=load mix test test/load/consumer_manager_restart_load_test.exs`).
  It should also be run as part of the full `test/load/` suite, but
  `Application.stop(:pulsar_ex)` in the second scenario is inherently
  disruptive to whatever else might be running in the same `mix test`
  invocation - if the standalone run passes but the combined run doesn't,
  that's a scheduling/isolation problem (e.g. this file needing to run last,
  or in its own invocation), not evidence of a product defect.
  """

  use ExUnit.Case

  alias PulsarEx.{
    LoadTopics,
    LoadRegexConsumer,
    LoadTiming,
    LoadFaultInjector,
    TestAdmin,
    ConsumerManager
  }

  # Small and single-partitioned on purpose: this file is about
  # crash/restart recovery correctness, not scale (already covered
  # elsewhere in this load harness).
  @topic_count 5
  @partitions 1
  @num_consumers 1

  # Shortened from the 60s default so this file doesn't need to wait
  # minutes for anything - recovery here is driven by explicit re-calls to
  # `start_consumer`, not by waiting out the refresh cadence.
  @refresh_interval 3_000

  @active_timeout 30_000
  @ready_timeout 30_000
  @window_timeout 30_000

  # A bare ConsumerManager crash is restarted by its supervisor almost
  # immediately; a full application restart involves re-initializing every
  # registry/supervisor and is given more room.
  @manager_restart_timeout 10_000
  @app_restart_active_timeout 60_000
  @app_restart_ready_timeout 60_000

  @moduletag timeout: :infinity

  test "crash_consumer_manager: manager bookkeeping is rebuilt and running consumers survive" do
    prefix = "ConsumerManagerRestartCrash#{System.unique_integer([:positive])}_"
    cluster = LoadRegexConsumer.cluster()
    tenant = LoadRegexConsumer.tenant()
    namespace = LoadRegexConsumer.namespace()
    subscription = LoadRegexConsumer.subscription()

    on_exit(fn -> LoadRegexConsumer.stop(Regex.compile!("^#{prefix}\\d+$")) end)

    {topic_regex, topics, desired_consumers} = provision_and_start(prefix)

    confirm_message_flow(topics, cluster, "pre-crash probe")

    pre_crash_active = PulsarEx.active_consumers(cluster, tenant, namespace, subscription)
    pre_crash_ready = PulsarEx.ready_consumers(cluster, tenant, namespace, subscription)
    assert pre_crash_active == desired_consumers
    assert pre_crash_ready == desired_consumers

    manager_pid_before = Process.whereis(PulsarEx.ConsumerManager)
    assert is_pid(manager_pid_before)

    :ok = LoadFaultInjector.crash_consumer_manager()

    wait_for_new_manager_pid(manager_pid_before, @manager_restart_timeout)

    # In-memory GenServer state is lost, not preserved. The
    # manager's own desired-count bookkeeping drops to 0 because its snapshots
    # were wiped by the crash - proof it was reset, not silently carried over.
    assert ConsumerManager.desired_consumers(cluster, tenant, namespace, subscription) == 0

    # Meanwhile the already-running consumers (owned by ConsumerSupervisor,
    # never touched by this crash - see moduledoc) kept serving without
    # interruption: active/ready counts read straight from
    # ConsumerIDRegistry/ConsumerReadyRegistry never dipped.
    assert PulsarEx.active_consumers(cluster, tenant, namespace, subscription) ==
             desired_consumers

    assert PulsarEx.ready_consumers(cluster, tenant, namespace, subscription) ==
             desired_consumers

    # The fresh ConsumerManager has no `timers` entry for this pattern, so
    # calling start again is a genuine discovery pass, not the Phase 3
    # "already running" no-op - proven by the desired count's 0 -> desired
    # transition below.
    restart_consumer_and_wait(
      topic_regex,
      desired_consumers,
      @active_timeout,
      @ready_timeout
    )

    assert ConsumerManager.desired_consumers(cluster, tenant, namespace, subscription) ==
             desired_consumers

    # No duplicate/stale registrations: DynamicSupervisor.start_child returns
    # {:error, {:already_started, _}} for every already-running
    # topic-partition (consumer_spec/5 registers via a unique
    # {cluster, topic, subscription} key in ConsumerRegistry), so re-running
    # discovery over already-started consumers must not double-count them.
    assert PulsarEx.active_consumers(cluster, tenant, namespace, subscription) ==
             desired_consumers

    assert PulsarEx.ready_consumers(cluster, tenant, namespace, subscription) ==
             desired_consumers

    confirm_message_flow(topics, cluster, "post-crash-recovery probe")

    LoadRegexConsumer.stop(topic_regex)
  end

  test "restart_application: process state is rebuilt from scratch" do
    prefix = "ConsumerManagerRestartApp#{System.unique_integer([:positive])}_"
    cluster = LoadRegexConsumer.cluster()
    tenant = LoadRegexConsumer.tenant()
    namespace = LoadRegexConsumer.namespace()
    subscription = LoadRegexConsumer.subscription()
    topic_regex = Regex.compile!("^#{prefix}\\d+$")

    on_exit(fn ->
      # A restart_application/0 failure mid-test would otherwise leave
      # :pulsar_ex down for every sibling test sharing this `mix test`
      # invocation - this is a last-resort safety net, not the primary
      # recovery path this test is actually verifying.
      Application.ensure_all_started(:pulsar_ex)
      LoadRegexConsumer.stop(topic_regex)
    end)

    {^topic_regex, topics, desired_consumers} = provision_and_start(prefix)

    confirm_message_flow(topics, cluster, "pre-restart probe")

    :ok = LoadFaultInjector.restart_application()

    # A real application restart takes down every process,
    # including ConsumerSupervisor's children and ConsumerManager's own
    # state - unlike the bare-crash scenario above, active/ready genuinely
    # go to 0 here, and nothing restores them without a fresh start_consumer
    # call.
    assert PulsarEx.active_consumers(cluster, tenant, namespace, subscription) == 0
    assert PulsarEx.ready_consumers(cluster, tenant, namespace, subscription) == 0
    assert ConsumerManager.desired_consumers(cluster, tenant, namespace, subscription) == 0

    # There is no "still running" pattern after a real application restart
    # (ConsumerManager's state, and every consumer process, are both gone),
    # so this start_consumer call must perform discovery from scratch and
    # succeed - not silently no-op the way a repeated start against a
    # still-running pattern would (Phase 3 behavior).
    restart_consumer_and_wait(
      topic_regex,
      desired_consumers,
      @app_restart_active_timeout,
      @app_restart_ready_timeout
    )

    assert PulsarEx.active_consumers(cluster, tenant, namespace, subscription) ==
             desired_consumers

    assert PulsarEx.ready_consumers(cluster, tenant, namespace, subscription) ==
             desired_consumers

    assert ConsumerManager.desired_consumers(cluster, tenant, namespace, subscription) ==
             desired_consumers

    # Confirms the rebuilt state actually works, not just that the counts
    # look right - real messages flow through the freshly (re)started
    # consumers and connections.
    confirm_message_flow(topics, cluster, "post-restart probe")

    LoadRegexConsumer.stop(topic_regex)
  end

  defp provision_and_start(prefix) do
    topic_regex = Regex.compile!("^#{prefix}\\d+$")

    LoadRegexConsumer.reset(topic_regex)

    topics = LoadTopics.provision(prefix, @topic_count, @partitions)

    for topic <- topics do
      TestAdmin.reset_subscription(topic, LoadRegexConsumer.subscription())
    end

    desired_consumers = @topic_count * @partitions * @num_consumers

    restart_consumer_and_wait(topic_regex, desired_consumers, @active_timeout, @ready_timeout)

    {topic_regex, topics, desired_consumers}
  end

  defp restart_consumer_and_wait(topic_regex, desired_consumers, active_timeout, ready_timeout) do
    assert :ok =
             LoadRegexConsumer.start(topic_regex,
               num_consumers: @num_consumers,
               refresh_interval: @refresh_interval
             )

    LoadRegexConsumer.wait_until_active(desired_consumers, active_timeout)
    LoadRegexConsumer.wait_until_ready(desired_consumers, ready_timeout)
  end

  defp confirm_message_flow(topics, cluster, label) do
    received_before = LoadRegexConsumer.received()

    for topic <- topics do
      assert {:ok, _} = PulsarEx.Cluster.produce(cluster, topic, label)
    end

    LoadTiming.wait_until(@window_timeout, fn ->
      LoadRegexConsumer.received() - received_before >= length(topics)
    end)
  end

  defp wait_for_new_manager_pid(old_pid, timeout_ms) do
    LoadTiming.wait_until(timeout_ms, fn ->
      case Process.whereis(PulsarEx.ConsumerManager) do
        pid when is_pid(pid) and pid != old_pid -> true
        _ -> false
      end
    end)
  end
end
