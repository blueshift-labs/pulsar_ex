defmodule PulsarEx.RegexConsumerStopConsumerTest do
  use ExUnit.Case, async: false

  alias PulsarEx.{TestRegexConsumer, TestAdmin, ConsumerRegistry, ConsumerSupervisor}

  @cluster "integration"
  @tenant "pulsar_ex"
  @namespace "IntegrationTest"

  @topic "persistent://pulsar_ex/IntegrationTest/RegexStopConsumerCrashTest"
  @topic_regex ~r/^RegexStopConsumerCrashTest$/

  setup do
    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()
    TestAdmin.reset_subscription(@topic, TestRegexConsumer.subscription())

    on_exit(fn ->
      ConsumerRegistry
      |> Registry.lookup({@cluster, @topic, TestRegexConsumer.subscription()})
      |> Enum.each(fn {pid, _} -> DynamicSupervisor.terminate_child(ConsumerSupervisor, pid) end)
    end)

    {:ok, []}
  end

  @tag timeout: :infinity
  test "stop_consumer terminates a registered consumer without crashing ConsumerManager" do
    for i <- 1..5 do
      assert {:ok, _} = PulsarEx.Cluster.produce(@cluster, @topic, "message #{i}")
    end

    assert :ok = TestRegexConsumer.start(@topic_regex)

    Process.sleep(10_000)

    assert [received: 5] = TestRegexConsumer.received()

    subscription = TestRegexConsumer.subscription()
    manager_before = Process.whereis(PulsarEx.ConsumerManager)

    assert [{consumer_pid, _}] =
             Registry.lookup(ConsumerRegistry, {@cluster, @topic, subscription})

    assert :ok =
             PulsarEx.Cluster.stop_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               subscription
             )

    assert Process.whereis(PulsarEx.ConsumerManager) == manager_before
    # DynamicSupervisor.terminate_child/2 (called by stop_consumer) blocks until this
    # pid has actually exited, so this is deterministic. ConsumerRegistry clears its
    # own entry via an independent monitor with no ordering guarantee relative to
    # terminate_child's return, so asserting the registry is empty here can be flaky
    # under load even though the consumer is genuinely gone.
    refute Process.alive?(consumer_pid)
  end

  @tag timeout: :infinity
  test "stop_consumer terminates a registered consumer after ConsumerManager restarts" do
    assert :ok = TestRegexConsumer.start(@topic_regex)

    subscription = TestRegexConsumer.subscription()

    assert [{consumer_pid, _}] =
             Registry.lookup(ConsumerRegistry, {@cluster, @topic, subscription})

    manager_before = Process.whereis(PulsarEx.ConsumerManager)
    Process.exit(manager_before, :kill)

    manager_after = wait_for_manager_restart(manager_before)

    assert manager_after != manager_before
    assert Process.alive?(consumer_pid)

    assert :ok =
             PulsarEx.Cluster.stop_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               subscription
             )

    refute Process.alive?(consumer_pid)
  end

  defp wait_for_manager_restart(previous_pid, attempts \\ 100)

  defp wait_for_manager_restart(_previous_pid, 0) do
    flunk("ConsumerManager did not restart")
  end

  defp wait_for_manager_restart(previous_pid, attempts) do
    case Process.whereis(PulsarEx.ConsumerManager) do
      pid when is_pid(pid) and pid != previous_pid ->
        pid

      _ ->
        Process.sleep(10)
        wait_for_manager_restart(previous_pid, attempts - 1)
    end
  end
end
