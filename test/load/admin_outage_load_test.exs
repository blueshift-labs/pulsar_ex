defmodule PulsarEx.AdminOutageLoadTest do
  use ExUnit.Case

  @moduledoc """
  Exercises the documented no-auto-retry discovery contract (regex/tenant-
  namespace consumer arity) under a simulated Admin API outage.

  This does not tear down any real network path to the broker's Admin HTTP
  port. A genuine network-level partition (blocking only Admin traffic while
  leaving the broker connection up) was investigated first and found
  impractical against the shared long-lived container: it has no `iptables`
  binary on its `PATH` and runs without the `NET_ADMIN` capability, and
  installing either into a shared container mid-run would introduce its own
  risk rather than fix anything. Instead, this swaps the `admin_module`
  config seam `consumer_manager.ex` already reads for `LoadFaultyAdmin`, a
  stub whose `discover_topics/5` returns an error on command. From pulsar_ex's
  point of view that stub failure is indistinguishable from a real Admin API
  call failing - which is exactly the failure mode the no-auto-retry contract
  is about, regardless of what caused the underlying HTTP call to fail.

  Three behaviors are proven here:

    1. An outage that hits mid-refresh does not disturb consumers already
       running against already-discovered topics - they keep receiving and
       acking messages the whole time - while each blocked refresh attempt
       still surfaces as discovery-error telemetry.
    2. Once Admin recovers, a topic that appeared while it was down gets
       picked up on the very next scheduled refresh, with no process restart
       and no extra call from the test - just the existing timer loop
       noticing on its own.
    3. Starting a brand-new pattern while Admin is down fails fast (bounded,
       not hanging until some much larger default timeout) and is not
       retried automatically - a caller has to call start again themselves.
  """

  alias PulsarEx.{
    LoadTopics,
    LoadRegexConsumer,
    LoadTiming,
    LoadTelemetryCounter,
    LoadFaultInjector,
    LoadFaultyAdmin
  }

  @partitions 1
  @num_consumers 1

  @refresh_interval 2_500
  @active_timeout 15_000
  @ready_timeout 15_000
  @window_timeout 15_000

  @moduletag timeout: :infinity

  setup do
    Application.put_env(:pulsar_ex, :admin_module, LoadFaultyAdmin)
    LoadFaultyAdmin.unblock()

    on_exit(fn ->
      LoadFaultyAdmin.unblock()
      Application.delete_env(:pulsar_ex, :admin_module)
    end)

    :ok
  end

  test "refresh-time Admin outage preserves existing consumers and emits discovery-error telemetry" do
    prefix = "AdminOutageRefresh#{System.unique_integer([:positive])}_"
    topic_regex = Regex.compile!("^#{prefix}\\d+$")

    on_exit(fn -> LoadRegexConsumer.stop(topic_regex) end)

    LoadRegexConsumer.reset(topic_regex)

    topics = LoadTopics.provision(prefix, 2, @partitions)
    expected_consumers = length(topics) * @partitions * @num_consumers

    assert :ok = LoadRegexConsumer.start(topic_regex, refresh_interval: @refresh_interval)

    LoadRegexConsumer.wait_until_active(expected_consumers, @active_timeout)
    LoadRegexConsumer.wait_until_ready(expected_consumers, @ready_timeout)

    # Confirm messages flow normally before the outage starts.
    for topic <- topics do
      assert {:ok, _} = PulsarEx.Cluster.produce(LoadRegexConsumer.cluster(), topic, "warmup")
    end

    LoadTiming.wait_until(@window_timeout, fn ->
      LoadRegexConsumer.received() >= length(topics)
    end)

    telemetry_handle = LoadTelemetryCounter.start()

    assert :ok = LoadFaultInjector.block_admin()

    # Cover at least one full refresh interval while Admin stays blocked.
    Process.sleep(@refresh_interval + 2_000)

    received_before_probe = LoadRegexConsumer.received()

    # Prove existing consumers keep actually working during the outage, not
    # just "didn't crash" - produce and confirm receipt of a fresh message on
    # every already-discovered topic while still blocked.
    for topic <- topics do
      assert {:ok, _} =
               PulsarEx.Cluster.produce(LoadRegexConsumer.cluster(), topic, "outage probe")
    end

    LoadTiming.wait_until(@window_timeout, fn ->
      LoadRegexConsumer.received() - received_before_probe >= length(topics)
    end)

    tallies = LoadTelemetryCounter.stop(telemetry_handle)

    discovery_error_count =
      get_in(tallies, [[:pulsar_ex, :consumer_manager, :discovery, :error], :count]) || 0

    assert discovery_error_count >= 1

    assert :ok = LoadFaultInjector.restore_admin()
  end

  test "a topic created during the outage is discovered on the next refresh once Admin is restored, with no manual intervention" do
    prefix = "AdminOutageRecovery#{System.unique_integer([:positive])}_"
    topic_regex = Regex.compile!("^#{prefix}\\d+$")

    on_exit(fn -> LoadRegexConsumer.stop(topic_regex) end)

    LoadRegexConsumer.reset(topic_regex)

    baseline_topics = LoadTopics.provision(prefix, 2, @partitions)
    baseline_expected = length(baseline_topics) * @partitions * @num_consumers

    assert :ok = LoadRegexConsumer.start(topic_regex, refresh_interval: @refresh_interval)

    LoadRegexConsumer.wait_until_active(baseline_expected, @active_timeout)

    active_before_new_topic =
      PulsarEx.active_consumers(
        LoadRegexConsumer.cluster(),
        LoadRegexConsumer.tenant(),
        LoadRegexConsumer.namespace(),
        LoadRegexConsumer.subscription()
      )

    assert :ok = LoadFaultInjector.block_admin()

    # A new topic matching the regex, created while Admin is blocked - it
    # can't be discovered yet, since discovery itself is failing.
    new_topics = LoadTopics.provision(prefix, 1, @partitions, length(baseline_topics))

    # Let at least one blocked refresh attempt pass, confirming the new topic
    # genuinely isn't picked up while Admin stays down.
    Process.sleep(@refresh_interval + 1_000)

    assert PulsarEx.active_consumers(
             LoadRegexConsumer.cluster(),
             LoadRegexConsumer.tenant(),
             LoadRegexConsumer.namespace(),
             LoadRegexConsumer.subscription()
           ) == active_before_new_topic

    assert :ok = LoadFaultInjector.restore_admin()

    total_expected = active_before_new_topic + length(new_topics) * @partitions * @num_consumers

    # No process restart, no extra API call from the test past this point -
    # purely the already-scheduled refresh timer picking the new topic up on
    # its own next scheduled pass.
    LoadRegexConsumer.wait_until_active(total_expected, @refresh_interval + 5_000)
  end

  test "initial start while Admin is blocked returns an error promptly, with no auto-retry" do
    prefix = "AdminOutageInitialStart#{System.unique_integer([:positive])}_"
    topic_regex = Regex.compile!("^#{prefix}\\d+$")

    on_exit(fn -> LoadRegexConsumer.stop(topic_regex) end)

    assert :ok = LoadFaultInjector.block_admin()

    {result, call_start, call_done} =
      LoadTiming.time_call(fn ->
        LoadRegexConsumer.start(topic_regex, refresh_interval: @refresh_interval)
      end)

    assert {:error, _reason} = result
    # Bounded well under the GenServer.call default timeout (5s) - this fails
    # fast because discover_topics itself fails fast, not because it happened
    # to squeak in under some large ceiling.
    assert call_done - call_start < 3_000

    assert :ok = LoadFaultInjector.restore_admin()
  end
end
