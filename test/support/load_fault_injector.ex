defmodule PulsarEx.LoadFaultInjector do
  @moduledoc """
  Shared fault-injection primitives for Phase 6.C's scenario tests: block/
  restore the Admin API, stop/start the `pulsar` broker container, crash the
  `ConsumerManager` GenServer, and restart the whole `:pulsar_ex` application.
  Centralizing these here means no individual scenario test reinvents its own
  `System.cmd("docker", ...)` calls or supervisor-introspection logic.

  Every function returns or raises clearly on failure rather than hanging a
  test suite silently - the same spirit as `LoadResourceSampler.os_rss_kb/0`'s
  "never crash/hang the harness on an external-tool failure" rule, except
  here the failure is surfaced (an `{:error, reason}` return or a raise)
  rather than swallowed into a sentinel: a fault-injection primitive that
  silently no-ops would let a scenario test produce a false-positive pass.
  """

  alias PulsarEx.LoadFaultyAdmin

  @default_healthcheck_timeout_ms 60_000
  @healthcheck_poll_interval_ms 2_000

  @doc "Marks the Admin API as blocked via the `admin_module` config seam (`LoadFaultyAdmin`)."
  def block_admin(), do: LoadFaultyAdmin.block()

  @doc "Restores the Admin API via the `admin_module` config seam (`LoadFaultyAdmin`)."
  def restore_admin(), do: LoadFaultyAdmin.unblock()

  @doc """
  Stops the `pulsar` broker container, stays down for `downtime_ms` (default
  `0`), then starts it again and polls the same healthcheck
  `docker-compose.yml` defines for the `pulsar` service
  (`bin/pulsar-admin brokers healthcheck`, expecting `ok` in its output) until
  it succeeds or `healthcheck_timeout_ms` (default
  #{@default_healthcheck_timeout_ms}ms) elapses.

  Returns `:ok` once healthy again. Returns `{:error, reason}` if either
  `docker` step fails outright, or `{:error, :healthcheck_timeout}` if the
  broker never reports healthy in time - never hangs forever waiting.
  """
  def restart_broker(downtime_ms \\ 0, healthcheck_timeout_ms \\ @default_healthcheck_timeout_ms) do
    case System.cmd("docker", ["stop", "pulsar"], stderr_to_stdout: true) do
      {_out, 0} ->
        Process.sleep(downtime_ms)
        do_start_broker(healthcheck_timeout_ms)

      {out, status} ->
        {:error, {:docker_stop_failed, status, out}}
    end
  end

  defp do_start_broker(healthcheck_timeout_ms) do
    case System.cmd("docker", ["start", "pulsar"], stderr_to_stdout: true) do
      {_out, 0} ->
        deadline = System.monotonic_time(:millisecond) + healthcheck_timeout_ms
        wait_for_broker_healthy(deadline)

      {out, status} ->
        {:error, {:docker_start_failed, status, out}}
    end
  end

  defp wait_for_broker_healthy(deadline) do
    case System.cmd("docker", ["exec", "pulsar", "bin/pulsar-admin", "brokers", "healthcheck"],
           stderr_to_stdout: true
         ) do
      {out, 0} ->
        if String.contains?(out, "ok") do
          :ok
        else
          retry_or_timeout(deadline)
        end

      {_out, _status} ->
        retry_or_timeout(deadline)
    end
  end

  defp retry_or_timeout(deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error, :healthcheck_timeout}
    else
      Process.sleep(@healthcheck_poll_interval_ms)
      wait_for_broker_healthy(deadline)
    end
  end

  @doc """
  Kills the `ConsumerManager` GenServer with `Process.exit(pid, :kill)` - a
  real, un-trappable crash, not `GenServer.stop/1` (which is a graceful
  shutdown) - so `PulsarEx.Supervisor`'s `rest_for_one` strategy (see
  `lib/pulsar_ex/application.ex`) actually has to restart it, and every
  sibling started after it in the child list.

  Raises if `ConsumerManager` isn't found among `PulsarEx.Supervisor`'s
  children (e.g. `:pulsar_ex` isn't started).
  """
  def crash_consumer_manager() do
    pid = consumer_manager_pid!()
    Process.exit(pid, :kill)
    :ok
  end

  defp consumer_manager_pid!() do
    PulsarEx.Supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {PulsarEx.ConsumerManager, pid, _type, _modules} when is_pid(pid) -> pid
      _ -> nil
    end) ||
      raise "ConsumerManager not found among PulsarEx.Supervisor's children - is :pulsar_ex started?"
  end

  @doc """
  Stops and restarts the whole `:pulsar_ex` application in the same BEAM
  (`Application.stop/1` then `Application.start/1`), rebuilding every
  registry/supervisor from scratch. Raises (via a failed `:ok =` match) if
  either step doesn't return `:ok`.
  """
  def restart_application() do
    :ok = Application.stop(:pulsar_ex)
    :ok = Application.start(:pulsar_ex)
  end
end
