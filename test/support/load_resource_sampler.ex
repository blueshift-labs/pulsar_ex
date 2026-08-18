defmodule PulsarEx.LoadResourceSampler do
  @moduledoc false

  # Periodic BEAM/OS resource sampler for the load harness (Phase 4, Task 4.3).
  # Records process count, VM memory, per-scheduler utilization, OS RSS, and
  # mailbox-length distributions (consumer processes, and separately their
  # supervisors) - all via cheap local reads (Registry.select/2 +
  # Process.info/2), never a GenServer.call into any consumer, so sampling
  # itself doesn't perturb the queues being measured.

  use GenServer

  alias PulsarEx.{ConsumerIDRegistry, ConsumerRegistry}

  @doc """
  Starts sampling on a `:timer.send_interval/2` tick of `interval_ms`. Returns
  `{:ok, pid}`; pass the pid to `stop/1` to stop sampling and collect results.
  """
  def start(interval_ms) do
    GenServer.start(__MODULE__, interval_ms)
  end

  @doc """
  Stops sampling and returns the accumulated list of samples, oldest first.
  """
  def stop(pid) do
    GenServer.call(pid, :stop)
  end

  def json_safe(samples, timestamp_offset \\ 0) do
    Enum.map(samples, fn sample ->
      %{
        sample
        | timestamp: sample.timestamp - timestamp_offset,
          memory: Map.new(sample.memory),
          mailbox_lengths: Enum.map(sample.mailbox_lengths, fn {_pid, len} -> len end),
          supervisor_mailbox_lengths:
            Enum.map(sample.supervisor_mailbox_lengths, fn {_pid, len} -> len end)
      }
    end)
  end

  @impl true
  def init(interval_ms) do
    :erlang.system_flag(:scheduler_wall_time, true)
    prev_snapshot = scheduler_wall_time_snapshot()

    {:ok, timer_ref} = :timer.send_interval(interval_ms, :sample)

    {:ok, %{prev_snapshot: prev_snapshot, timer_ref: timer_ref, samples: []}}
  end

  @impl true
  def handle_info(:sample, %{prev_snapshot: prev_snapshot, samples: samples} = state) do
    current_snapshot = scheduler_wall_time_snapshot()

    sample = %{
      timestamp: System.monotonic_time(:millisecond),
      process_count: :erlang.system_info(:process_count),
      memory: :erlang.memory(),
      scheduler_utilization: scheduler_utilization(prev_snapshot, current_snapshot),
      rss_kb: os_rss_kb(),
      mailbox_lengths: mailbox_lengths(ConsumerIDRegistry),
      supervisor_mailbox_lengths: mailbox_lengths(ConsumerRegistry)
    }

    {:noreply, %{state | prev_snapshot: current_snapshot, samples: [sample | samples]}}
  end

  @impl true
  def handle_call(:stop, _from, %{timer_ref: timer_ref, samples: samples} = state) do
    _ = :timer.cancel(timer_ref)
    {:stop, :normal, Enum.reverse(samples), state}
  end

  defp scheduler_wall_time_snapshot() do
    :erlang.statistics(:scheduler_wall_time) |> Enum.sort()
  end

  defp scheduler_utilization(prev_snapshot, current_snapshot) do
    prev_by_id = Map.new(prev_snapshot, fn {id, active, total} -> {id, {active, total}} end)

    Enum.map(current_snapshot, fn {id, active, total} ->
      {prev_active, prev_total} = Map.fetch!(prev_by_id, id)
      delta_total = total - prev_total

      utilization =
        if delta_total > 0 do
          (active - prev_active) / delta_total
        else
          0.0
        end

      %{scheduler_id: id, utilization: utilization}
    end)
  end

  # A single bad `ps` invocation (missing binary, unexpected output shape)
  # must not crash the sampler and discard every sample accumulated so far
  # during a long-running scenario - so this reports `nil` on failure instead
  # of raising.
  defp os_rss_kb() do
    with {output, 0} <- System.cmd("ps", ["-o", "rss=", "-p", System.pid()]),
         {rss_kb, _rest} <- Integer.parse(String.trim(output)) do
      rss_kb
    else
      _ -> nil
    end
  end

  # Registry.select/2 is a local, non-message-passing read of the registry's
  # internal ETS table, and Process.info/2 is a cheap local BIF read - neither
  # sends a message to the process being measured (Phase 2's no-fan-out rule:
  # no per-consumer GenServer.call round-trips here). `ConsumerIDRegistry`
  # holds the individual consumer GenServer pids (see
  # `PulsarEx.active_consumers/4`, `lib/pulsar_ex/consumer.ex:168-187`) - the
  # processes that actually receive and queue Pulsar messages, hence the
  # mailbox lengths that matter for backpressure. `ConsumerRegistry` holds one
  # Supervisor pid per `{cluster, topic, subscription}` (see
  # `lib/pulsar_ex/consumer_manager.ex:625-637`) - its mailboxes are supervisor
  # traffic, not consumer backpressure, but it's sampled too since the task
  # doc's fake-consumer verification registers into it.
  defp mailbox_lengths(registry) do
    registry
    |> Registry.select([{{:_, :"$1", :_}, [], [:"$1"]}])
    |> Enum.flat_map(fn pid ->
      case Process.info(pid, :message_queue_len) do
        {:message_queue_len, len} -> [{pid, len}]
        nil -> []
      end
    end)
  end
end
