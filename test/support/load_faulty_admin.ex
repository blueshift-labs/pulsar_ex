defmodule PulsarEx.LoadFaultyAdmin do
  @moduledoc """
  A config-seam `admin_module` double that simulates Admin API failure, for
  Phase 6's fault-injection scenarios (Task 6.C.1, "Temporary Admin outage").

  **This simulates Admin API failure at the pulsar_ex config-seam boundary
  (`Application.put_env(:pulsar_ex, :admin_module, LoadFaultyAdmin)`), not a
  real network partition.** The original plan called for an in-container
  `docker exec pulsar iptables -A INPUT -p tcp --dport 8080 -j DROP` rule, to
  block only Admin (8080) traffic while the broker connection (6650) stayed
  up. That was checked directly against the running `pulsar` container before
  any implementation work started, and confirmed infeasible:
  `docker exec pulsar iptables -L -n` fails with `exec: "iptables": executable
  file not found in $PATH`, and `docker inspect pulsar` shows `CapAdd: []` /
  `Privileged: false` - no `NET_ADMIN`, and installing `iptables` into a
  long-lived shared container mid-run would be its own new risk, not a fix.

  Instead, this module makes `discover_topics/5` fail on command while
  "blocked". That still proves what the no-auto-retry contract (Phase 3, Task
  3.2) actually cares about - pulsar_ex's handling of a failing Admin API
  call - regardless of the failure's transport-level cause.

  Uses its own ETS table (`:load_faulty_admin`), not `PulsarEx.TestAdmin`'s -
  different lifecycle/purpose: `TestAdmin` overrides discovery results
  per-key for deterministic regex-match tests; this module only toggles one
  global blocked/unblocked flag to simulate an outage window.

  Callers own wiring this module into the `admin_module` config seam
  themselves (typically `Application.put_env(:pulsar_ex, :admin_module,
  LoadFaultyAdmin)` in a scenario test's `setup`, and
  `Application.delete_env(:pulsar_ex, :admin_module)` in `on_exit`) - this
  module only owns the blocked flag, matching how `TestAdmin` doesn't wire its
  own config either.
  """

  alias PulsarEx.Admin

  @table :load_faulty_admin
  @key :blocked

  @doc "Marks Admin as blocked - subsequent `discover_topics/5` calls fail immediately."
  def block() do
    ensure_table()
    :ets.insert(@table, {@key, true})
    :ok
  end

  @doc "Marks Admin as unblocked - subsequent `discover_topics/5` calls delegate to the real Admin."
  def unblock() do
    ensure_table()
    :ets.insert(@table, {@key, false})
    :ok
  end

  @doc "Returns whether Admin is currently marked blocked (defaults to `false` if never toggled)."
  def blocked?() do
    ensure_table()

    case :ets.lookup(@table, @key) do
      [{@key, blocked}] -> blocked
      [] -> false
    end
  end

  @doc """
  Returns `{:error, :simulated_admin_outage}` while blocked; otherwise
  delegates straight to the real `PulsarEx.Admin.discover_topics/5`.

  `discover_topics/5` is the only function `lib/pulsar_ex/consumer_manager.ex`'s
  `admin_module()` seam ever calls today (confirmed via `grep -n
  "admin_module()" lib/pulsar_ex/consumer_manager.ex`), so no other `Admin`
  function needs a faulty counterpart here.
  """
  def discover_topics(brokers, admin_port, tenant, namespace, topic_name_or_regex) do
    if blocked?() do
      {:error, :simulated_admin_outage}
    else
      Admin.discover_topics(brokers, admin_port, tenant, namespace, topic_name_or_regex)
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
end
