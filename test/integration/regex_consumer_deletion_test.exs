defmodule PulsarEx.RegexConsumerDeletionTest do
  use ExUnit.Case, async: false

  alias PulsarEx.{TestRegexConsumer, TestAdmin, ConsumerRegistry}

  @cluster "integration"
  @tenant "pulsar_ex"
  @namespace "IntegrationTest"

  @topic_keep "persistent://pulsar_ex/IntegrationTest/RegexDeletionTestKeep"
  @topic_drop "persistent://pulsar_ex/IntegrationTest/RegexDeletionTestDrop"
  @topic_regex ~r/^RegexDeletionTest(Keep|Drop)$/

  @subscription "regex_consumer_deletion_test"
  @refresh_interval 3_000

  setup do
    Application.put_env(:pulsar_ex, :admin_module, TestAdmin)
    TestAdmin.clear_overrides()

    on_exit(fn ->
      PulsarEx.Cluster.stop_consumer(@cluster, @tenant, @namespace, @topic_regex, @subscription)
      Application.delete_env(:pulsar_ex, :admin_module)
    end)

    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()
    TestAdmin.reset_subscription(@topic_keep, @subscription)
    TestAdmin.reset_subscription(@topic_drop, @subscription)

    {:ok, []}
  end

  # A previously-matched topic that drops out of a later discover_topics result
  # (simulating deletion) has its local consumer stopped on the next refresh,
  # while the surviving topic's consumer is left running undisturbed.
  @tag timeout: :infinity
  test "a topic dropped from discovery on refresh is stopped, the surviving topic keeps consuming" do
    for i <- 1..10 do
      assert {:ok, _} = PulsarEx.Cluster.produce(@cluster, @topic_keep, "message #{i}")
      assert {:ok, _} = PulsarEx.Cluster.produce(@cluster, @topic_drop, "message #{i}")
    end

    TestAdmin.override(@tenant, @namespace, @topic_regex, {:ok, [@topic_keep, @topic_drop]})

    assert :ok =
             PulsarEx.Cluster.start_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               @subscription,
               TestRegexConsumer,
               initial_position: :earliest,
               refresh_interval: @refresh_interval
             )

    Process.sleep(5_000)

    assert [received: 20] = TestRegexConsumer.received()

    # Simulate the drop topic disappearing from discovery (e.g. it was deleted).
    TestAdmin.override(@tenant, @namespace, @topic_regex, {:ok, [@topic_keep]})

    Process.sleep(@refresh_interval + 2_000)

    assert [] = Registry.lookup(ConsumerRegistry, {@cluster, @topic_drop, @subscription})

    assert {:ok, _} =
             PulsarEx.Cluster.produce(@cluster, @topic_keep, "message after reconciliation")

    Process.sleep(5_000)

    assert [received: 21] = TestRegexConsumer.received()
  end
end
