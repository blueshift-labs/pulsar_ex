defmodule PulsarEx.BrokerRestartLoadTest do
  use ExUnit.Case

  @moduledoc """
  Exercises consumer behavior across a full restart of the Pulsar broker
  container: connection loss while the broker is unreachable, and
  reconnect/resume once it is healthy again.

  This local setup runs a single standalone Pulsar container, not a
  multi-broker cluster - the multi-broker compose services in this repo exist
  but nothing points at them. So "broker restart" here means that one
  container going down and coming back, and this test can only prove
  single-node full-outage reconnect behavior. It says nothing about
  multi-broker failover (leader election, replica catch-up, etc.) - this
  local harness has no way to exercise that.

  This test genuinely stops and restarts the shared `pulsar` container for
  real (`docker stop`/`docker start`, polled via its own healthcheck command)
  - it cannot run concurrently with any other `test/load/` file that also
  needs the broker reachable. Run it standalone. Between the intentional
  downtime, the container's own restart time, and the retry backoff, a full
  run takes on the order of two to three minutes.

  Every consumer child is registered `restart: :permanent` under its own
  per-topic-partition supervisor, and a `docker stop` kills the
  current connection out from under the consumer (`{:DOWN, ...}` ->
  `[:pulsar_ex, :consumer, :connection_down]` -> `{:stop, {:shutdown,
  :connection_down}, state}`), which its supervisor immediately replaces with
  a fresh pid. So individual consumer worker pids do cycle during the
  outage - that is expected, supervised churn, not a defect. What must NOT
  happen is a crash cascading up past that per-partition supervisor: this
  test asserts `ConsumerManager`/`PartitionManager`/`ConnectionManager` (the
  shared, cluster-wide processes) keep the exact same pid throughout, and
  that the target topic-partition's registry entry is never absent when
  polled.
  """

  alias PulsarEx.{
    LoadTopics,
    LoadRegexConsumer,
    LoadTiming,
    LoadTelemetryCounter,
    LoadFaultInjector,
    TestAdmin
  }

  @partitions 1
  @num_consumers 1

  # Default max_attempts is 10; against real broker-restart timing (a real
  # docker stop/start plus healthcheck poll, not a synthetic delay) exhausting
  # 10 attempts would take roughly a minute by itself, on top of however long
  # the container actually takes to come back. Lowered so retry exhaustion is
  # reachable within a single short outage without needlessly stretching this
  # test out. Left high enough to avoid tripping the per-topic-partition
  # supervisor's default restart intensity limit (3 restarts / 5s).
  @max_attempts 3

  @refresh_interval 10_000

  @active_timeout 20_000
  @ready_timeout 20_000
  @window_timeout 20_000

  # A few seconds of real downtime is enough to prove the point - the broker
  # doesn't need to stay down for a long time for reconnect behavior to be
  # observable, and the container's own restart time (JVM boot, not
  # under this test's control) adds a further, larger chunk of unavailability
  # on top of this regardless.
  @downtime_ms 15_000

  # This standalone container's healthcheck defines a 30s start_period before
  # Docker even begins polling it, on top of whatever the Pulsar standalone
  # process itself needs to finish booting. 90s gives that comfortable
  # headroom without waiting anywhere near this test's own hard ceiling.
  @healthcheck_timeout_ms 90_000

  # Sized generously so a slow (but correct) reconnect after the container
  # becomes healthy is not mistaken for a stuck one.
  @post_recovery_timeout_ms 120_000

  @moduletag timeout: :infinity

  test "consumers survive a full broker restart and reconnect once healthy" do
    prefix = "BrokerRestart#{System.unique_integer([:positive])}_"
    topic_regex = Regex.compile!("^#{prefix}\\d+$")

    on_exit(fn -> LoadRegexConsumer.stop(topic_regex) end)

    LoadRegexConsumer.reset(topic_regex)

    cluster = LoadRegexConsumer.cluster()
    tenant = LoadRegexConsumer.tenant()
    namespace = LoadRegexConsumer.namespace()
    subscription = LoadRegexConsumer.subscription()

    [topic] = LoadTopics.provision(prefix, 1, @partitions)
    TestAdmin.reset_subscription(topic, subscription)

    expected_consumers = @partitions * @num_consumers

    assert :ok =
             LoadRegexConsumer.start(topic_regex,
               num_consumers: @num_consumers,
               refresh_interval: @refresh_interval,
               max_attempts: @max_attempts
             )

    LoadRegexConsumer.wait_until_active(expected_consumers, @active_timeout)
    LoadRegexConsumer.wait_until_ready(expected_consumers, @ready_timeout)

    # Prove the consumer is actively receiving before the fault is injected,
    # not just registered as ready.
    received_before_outage = LoadRegexConsumer.received()
    assert {:ok, _} = PulsarEx.Cluster.produce(cluster, topic, "warmup")

    LoadTiming.wait_until(@window_timeout, fn ->
      LoadRegexConsumer.received() > received_before_outage
    end)

    shared_pids_before = shared_process_pids()

    telemetry_handle = LoadTelemetryCounter.start()

    fault_task =
      Task.async(fn -> LoadFaultInjector.restart_broker(@downtime_ms, @healthcheck_timeout_ms) end)

    {samples, restart_result} =
      poll_during_outage(fault_task, cluster, tenant, namespace, subscription)

    assert restart_result == :ok,
           "LoadFaultInjector.restart_broker/2 did not report the broker healthy again: #{inspect(restart_result)}"

    # The shared, cluster-wide processes must never have restarted - retry
    # exhaustion in an individual consumer must not cascade past its own
    # per-partition supervisor.
    assert shared_process_pids() == shared_pids_before

    # `active_consumers/4` counts `ConsumerIDRegistry` entries, which are
    # registered at `start_link` time (before `init/1` even runs) - so this
    # must never drop below the expected count, even while the pid behind
    # that registration keeps cycling underneath. This is the "no consumer
    # process exits or crashes" property, precisely stated: the registry slot
    # is always occupied, not that any specific pid is immortal.
    assert Enum.all?(samples, fn %{active: active} -> active >= expected_consumers end),
           "active consumer count dropped below #{expected_consumers} at least once during the outage: #{inspect(samples)}"

    tallies = LoadTelemetryCounter.stop(telemetry_handle)

    connection_down_count =
      get_in(tallies, [[:pulsar_ex, :consumer, :connection_down], :count]) || 0

    lookup_error_count = get_in(tallies, [[:pulsar_ex, :consumer, :lookup, :error], :count]) || 0

    connect_error_count =
      get_in(tallies, [[:pulsar_ex, :consumer, :connect, :error], :count]) || 0

    assert connection_down_count + lookup_error_count + connect_error_count >= 1,
           "expected at least one connection-down/lookup-error/connect-error event during the outage"

    max_attempts_count = get_in(tallies, [[:pulsar_ex, :consumer, :max_attempts], :count]) || 0

    assert max_attempts_count >= 1,
           "expected [:pulsar_ex, :consumer, :max_attempts] telemetry to fire at least once"

    # Recovery: gate on `ready`, not `active` - `active` never left
    # `expected_consumers` during the outage (see above), so it proves
    # nothing here. `ready` is registered only on a successful subscribe and
    # is auto-dropped on every process death, so it legitimately fell to 0
    # while the broker was down; seeing it climb back is the real signal that
    # the consumer actually resubscribed, not just that a pid exists.
    LoadRegexConsumer.wait_until_ready(expected_consumers, @post_recovery_timeout_ms)

    received_before_probe = LoadRegexConsumer.received()
    assert {:ok, _} = PulsarEx.Cluster.produce(cluster, topic, "recovery probe")

    LoadTiming.wait_until(@window_timeout, fn ->
      LoadRegexConsumer.received() > received_before_probe
    end)

    LoadRegexConsumer.stop(topic_regex)
  end

  defp shared_process_pids() do
    PulsarEx.Supervisor
    |> Supervisor.which_children()
    |> Enum.filter(fn {mod, _pid, _type, _mods} ->
      mod in [PulsarEx.ConsumerManager, PulsarEx.PartitionManager, PulsarEx.ConnectionManager]
    end)
    |> Map.new(fn {mod, pid, _type, _mods} -> {mod, pid} end)
  end

  # Samples active-consumer count once per `poll_interval_ms` while `task`
  # (the in-flight `restart_broker/2` call)
  # is still running, via `Task.yield/2` as the poll clock. Returns the
  # collected samples plus whatever `task` itself returned.
  defp poll_during_outage(
         task,
         cluster,
         tenant,
         namespace,
         subscription,
         poll_interval_ms \\ 500
       ) do
    do_poll_during_outage(
      task,
      cluster,
      tenant,
      namespace,
      subscription,
      poll_interval_ms,
      []
    )
  end

  defp do_poll_during_outage(
         task,
         cluster,
         tenant,
         namespace,
         subscription,
         poll_interval_ms,
         acc
       ) do
    sample = %{
      active: PulsarEx.active_consumers(cluster, tenant, namespace, subscription)
    }

    acc = [sample | acc]

    case Task.yield(task, poll_interval_ms) do
      nil ->
        do_poll_during_outage(
          task,
          cluster,
          tenant,
          namespace,
          subscription,
          poll_interval_ms,
          acc
        )

      {:ok, result} ->
        {Enum.reverse(acc), result}

      {:exit, reason} ->
        {Enum.reverse(acc), {:error, {:task_exit, reason}}}
    end
  end
end
