defmodule PulsarEx.RegexConsumerStopIndependentOfDiscoveryTest do
  use ExUnit.Case, async: false

  alias PulsarEx.{TestRegexConsumer, TestAdmin, ConsumerRegistry}

  @cluster "integration"
  @tenant "pulsar_ex"
  @namespace "IntegrationTest"

  @topic "persistent://pulsar_ex/IntegrationTest/RegexStopIndependentTest"
  @topic_regex ~r/^RegexStopIndependentTest$/

  @subscription "regex_consumer_stop_independent_of_discovery_test"

  setup do
    Application.put_env(:pulsar_ex, :admin_module, TestAdmin)
    TestAdmin.clear_overrides()

    on_exit(fn ->
      PulsarEx.Cluster.stop_consumer(@cluster, @tenant, @namespace, @topic_regex, @subscription)
      Application.delete_env(:pulsar_ex, :admin_module)
    end)

    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()
    TestAdmin.reset_subscription(@topic, @subscription)

    {:ok, []}
  end

  # stop_consumer must not depend on a live discover_topics call succeeding: it
  # stops consumers from the pattern's stored matched-topic set, not from a fresh
  # discovery result. Start against the real topic (discovery falls through to the
  # real Admin), confirm it is genuinely running, then make discovery for this
  # pattern fail/empty (simulating the topic becoming unreachable for discovery,
  # e.g. deleted) and confirm stop_consumer still tears down the real consumer.
  @tag timeout: :infinity
  test "stop_consumer stops a running consumer even when discovery for its pattern fails" do
    for i <- 1..5 do
      assert {:ok, _} = PulsarEx.Cluster.produce(@cluster, @topic, "message #{i}")
    end

    assert :ok =
             PulsarEx.Cluster.start_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               @subscription,
               TestRegexConsumer,
               initial_position: :earliest
             )

    Process.sleep(5_000)

    assert [received: 5] = TestRegexConsumer.received()

    assert [{consumer_pid, _}] =
             Registry.lookup(ConsumerRegistry, {@cluster, @topic, @subscription})

    # A subsequent discover_topics call for this pattern would now fail, as it
    # would if the topic had genuinely been deleted from discovery's perspective.
    TestAdmin.override(@tenant, @namespace, @topic_regex, {:error, :simulated_admin_down})

    assert :ok =
             PulsarEx.Cluster.stop_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               @subscription
             )

    # DynamicSupervisor.terminate_child/2 (called by stop_consumer) blocks until this
    # pid has actually exited, so this is deterministic. ConsumerRegistry clears its
    # own entry via an independent monitor with no ordering guarantee relative to
    # terminate_child's return, so asserting the registry is empty here can be flaky
    # under load even though the consumer is genuinely gone.
    refute Process.alive?(consumer_pid)
  end
end
