defmodule PulsarEx.ConsumerReadyRegistryTest do
  use ExUnit.Case

  alias PulsarEx.{ConsumerReadyRegistry, LoadTiming}

  test "a registered entry is counted, and automatically dropped when the registering process dies - including a brutal kill, with no manual decrement" do
    key =
      {"unit", "consumer_ready_registry_test_tenant", "consumer_ready_registry_test_ns",
       "test_sub"}

    test_pid = self()

    {:ok, pid} =
      Task.start(fn ->
        Registry.register(ConsumerReadyRegistry, key, nil)
        send(test_pid, :registered)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :registered
    assert length(Registry.lookup(ConsumerReadyRegistry, key)) == 1

    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

    # Registry's own process-monitoring drops the entry on death - this is the whole point
    # of using Registry instead of an ETS counter or a telemetry-event-driven decrement,
    # both of which would leak on a brutal kill (no terminate/2 runs, no event fires).
    LoadTiming.wait_until(
      200,
      fn -> Registry.lookup(ConsumerReadyRegistry, key) == [] end,
      10
    )

    assert Registry.lookup(ConsumerReadyRegistry, key) == []
  end
end
