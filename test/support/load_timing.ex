defmodule PulsarEx.LoadTiming do
  @moduledoc """
  Shared timing/polling helpers for the load test harness (see test/load).

  Centralizes the `wait_until/2` polling loop that was previously
  duplicated across load test files, and adds an analogous poll for
  active consumers (via `PulsarEx.active_consumers/4`) alongside the
  existing ready-consumer poll.
  """

  import ExUnit.Assertions, only: [flunk: 1]

  @default_poll_interval_ms 200

  @doc """
  Polls `fun` (a zero-arg function) until it returns truthy, sleeping
  `poll_interval_ms` between attempts. Returns the monotonic time (ms)
  at which `fun` first returned truthy. Flunks if `timeout_ms` elapses
  first.
  """
  def wait_until(timeout_ms, fun, poll_interval_ms \\ @default_poll_interval_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_until_loop(deadline, fun, poll_interval_ms)
  end

  defp wait_until_loop(deadline, fun, poll_interval_ms) do
    cond do
      fun.() ->
        System.monotonic_time(:millisecond)

      System.monotonic_time(:millisecond) > deadline ->
        flunk("condition not met before deadline")

      true ->
        Process.sleep(poll_interval_ms)
        wait_until_loop(deadline, fun, poll_interval_ms)
    end
  end

  @doc """
  Times a zero-arg function's wall-clock execution, e.g. a
  `start_consumer` call. Returns `{result, call_start_ms, call_done_ms}`
  (monotonic timestamps).
  """
  def time_call(fun) do
    call_start = System.monotonic_time(:millisecond)
    result = fun.()
    call_done = System.monotonic_time(:millisecond)

    {result, call_start, call_done}
  end

  @doc """
  Polls `PulsarEx.active_consumers/4` until it reaches `desired_count`.
  Returns the monotonic time (ms) at which it was reached.
  """
  def wait_until_active(cluster, tenant, namespace, subscription, desired_count, timeout_ms) do
    wait_until(timeout_ms, fn ->
      PulsarEx.active_consumers(cluster, tenant, namespace, subscription) >= desired_count
    end)
  end

  @doc """
  Polls `PulsarEx.ready_consumers/4` until it reaches `desired_count`.
  Returns the monotonic time (ms) at which it was reached.

  Callers should poll `wait_until_active/6` first: a consumer only
  becomes ready after it is active, so calling this after the active
  poll has already returned guarantees the returned timestamp is never
  earlier than the active one.
  """
  def wait_until_ready(cluster, tenant, namespace, subscription, desired_count, timeout_ms) do
    wait_until(timeout_ms, fn ->
      PulsarEx.ready_consumers(cluster, tenant, namespace, subscription) >= desired_count
    end)
  end
end
