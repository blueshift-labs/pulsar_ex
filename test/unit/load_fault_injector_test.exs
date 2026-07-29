defmodule PulsarEx.LoadFaultInjectorTest do
  @moduledoc """
  Covers only the fault primitives that don't need a real broker:
  `crash_consumer_manager/0`, `restart_application/0`, and `LoadFaultyAdmin`'s
  block/unblock/`blocked?/0`/`discover_topics/5` flag behavior.

  `restart_broker/1` is intentionally not covered here - it shells out to the
  real `pulsar` docker container, and that primitive is smoke-tested manually
  against the live container, not from this unit suite.

  Deliberately does not exercise `LoadFaultyAdmin.discover_topics/5`'s
  "delegates to the real `Admin`" branch: `config/test.exs` points
  `admin_port` at an invalid port (`80800`) specifically so `test/unit/` never
  hits a real broker. That branch is exercised later by the load-env
  Task 6.C.1 scenario test, which runs under `MIX_ENV=load` against the real
  Admin port (`8080`).

  `async: false` - this manipulates global application/process state. The
  `restart_application/0` test tears down every registry/supervisor in the
  app for the duration of the test, and the `crash_consumer_manager/0` test
  kills and leaves-restarted the live `ConsumerManager` singleton; if ExUnit
  ever ran this file concurrently with another file that assumes a
  continuously-running `:pulsar_ex` application or a stable `ConsumerManager`
  pid, that other file's tests could fail or hang. This passes standalone
  (`mix test test/unit/load_fault_injector_test.exs`, confirmed: 4 tests, 0
  failures). Note for whoever runs the full suite next: a `MIX_ENV=test mix
  test` run with *this file removed* was independently observed exceeding
  180s with no progress output, so any slowness/stall in the combined run is
  a pre-existing property of the full `test/unit/` suite (most likely
  `producer_test.exs`'s cumulative `Process.sleep/1` calls), not something
  introduced here - confirm against a clean full-suite baseline before
  attributing a stall to this file.
  """

  use ExUnit.Case, async: false

  alias PulsarEx.{LoadFaultInjector, LoadFaultyAdmin}

  setup do
    on_exit(fn -> LoadFaultyAdmin.unblock() end)
    :ok
  end

  describe "crash_consumer_manager/0" do
    test "the supervisor restarts ConsumerManager under a new pid, and it keeps working" do
      old_pid = consumer_manager_pid()
      assert is_pid(old_pid)

      assert :ok = LoadFaultInjector.crash_consumer_manager()

      new_pid = wait_for_new_consumer_manager_pid(old_pid)
      assert is_pid(new_pid)
      assert new_pid != old_pid
      assert Process.alive?(new_pid)

      # ConsumerManager answers calls post-crash; an unknown desired-consumer
      # key returns 0 without crashing or timing out.
      assert PulsarEx.ConsumerManager.desired_consumers(
               "unit",
               "no_such_tenant",
               "no_such_namespace",
               "no_such_subscription"
             ) == 0
    end
  end

  describe "restart_application/0" do
    test "stops and restarts :pulsar_ex in the same BEAM" do
      assert :pulsar_ex in started_app_names()

      assert :ok = LoadFaultInjector.restart_application()

      assert :pulsar_ex in started_app_names()
      assert is_pid(consumer_manager_pid())
    end
  end

  describe "LoadFaultyAdmin" do
    test "block/0, unblock/0, blocked?/0 toggle the flag" do
      LoadFaultyAdmin.unblock()
      refute LoadFaultyAdmin.blocked?()

      assert :ok = LoadFaultyAdmin.block()
      assert LoadFaultyAdmin.blocked?()

      assert :ok = LoadFaultyAdmin.unblock()
      refute LoadFaultyAdmin.blocked?()
    end

    test "discover_topics/5 returns the simulated-outage error while blocked" do
      LoadFaultyAdmin.block()

      assert LoadFaultyAdmin.discover_topics(["localhost"], 8080, "tenant", "namespace", "topic") ==
               {:error, :simulated_admin_outage}
    end
  end

  defp consumer_manager_pid() do
    PulsarEx.Supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {PulsarEx.ConsumerManager, pid, _type, _modules} when is_pid(pid) -> pid
      _ -> nil
    end)
  end

  defp wait_for_new_consumer_manager_pid(old_pid, attempts \\ 50)

  defp wait_for_new_consumer_manager_pid(old_pid, attempts) when attempts > 0 do
    case consumer_manager_pid() do
      ^old_pid ->
        Process.sleep(100)
        wait_for_new_consumer_manager_pid(old_pid, attempts - 1)

      pid ->
        pid
    end
  end

  defp wait_for_new_consumer_manager_pid(_old_pid, 0), do: nil

  defp started_app_names() do
    Application.started_applications() |> Enum.map(&elem(&1, 0))
  end
end
