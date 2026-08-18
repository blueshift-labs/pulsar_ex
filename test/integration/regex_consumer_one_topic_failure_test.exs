defmodule PulsarEx.RegexConsumerOneTopicFailureTest do
  use ExUnit.Case, async: false

  alias PulsarEx.{ConsumerRegistry, TestRegexConsumer, TestAdmin, TestPartitionManager}

  @cluster "integration"
  @tenant "pulsar_ex"
  @namespace "IntegrationTest"

  @topic_first "persistent://pulsar_ex/IntegrationTest/RegexOneTopicFailureTestFirst"
  @topic_bad "persistent://pulsar_ex/IntegrationTest/RegexOneTopicFailureTestBad"
  @topic_last "persistent://pulsar_ex/IntegrationTest/RegexOneTopicFailureTestLast"
  @topic_regex ~r/^RegexOneTopicFailureTest(First|Bad|Last)$/

  @subscription "regex_consumer_one_topic_failure_test"

  setup do
    Application.put_env(:pulsar_ex, :admin_module, TestAdmin)
    Application.put_env(:pulsar_ex, :partition_manager_module, TestPartitionManager)
    TestAdmin.clear_overrides()
    TestPartitionManager.clear_overrides()

    on_exit(fn ->
      PulsarEx.Cluster.stop_consumer(
        @cluster,
        @tenant,
        @namespace,
        @topic_regex,
        @subscription
      )

      Application.delete_env(:pulsar_ex, :admin_module)
      Application.delete_env(:pulsar_ex, :partition_manager_module)
    end)

    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()
    TestAdmin.reset_subscription(@topic_first, @subscription)
    TestAdmin.reset_subscription(@topic_last, @subscription)

    {:ok, []}
  end

  # `do_start_consumers/7`'s reduce attempts every discovered topic regardless of an
  # earlier failure, so a topic ordered after a failing one still starts normally.
  @tag timeout: :infinity
  test "a topic ordered after a failing topic still starts" do
    for i <- 1..10 do
      assert {:ok, _} = PulsarEx.Cluster.produce(@cluster, @topic_first, "message #{i}")
      assert {:ok, _} = PulsarEx.Cluster.produce(@cluster, @topic_last, "message #{i}")
    end

    TestAdmin.override(
      @tenant,
      @namespace,
      @topic_regex,
      {:ok, [@topic_first, @topic_bad, @topic_last]}
    )

    TestPartitionManager.override(
      @cluster,
      @topic_bad,
      {:error, :simulated_partition_lookup_failure}
    )

    assert {:error, :simulated_partition_lookup_failure} =
             PulsarEx.Cluster.start_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               @subscription,
               TestRegexConsumer,
               initial_position: :earliest
             )

    Process.sleep(10_000)

    # The bad topic's failure is isolated: both the first topic (ordered before it) and
    # the last topic (ordered after it) get consumers started and deliver their messages.
    assert [received: 20] = TestRegexConsumer.received()

    consumer_pids =
      for topic <- [@topic_first, @topic_last] do
        assert [{pid, _value}] =
                 Registry.lookup(ConsumerRegistry, {@cluster, topic, @subscription})

        pid
      end

    # Even though start returned the isolated topic error, the pattern is now
    # partially active. Its lifecycle state must still be retained so a later
    # regex stop can clean up every healthy topic that did start.
    assert :ok =
             PulsarEx.Cluster.stop_consumer(
               @cluster,
               @tenant,
               @namespace,
               @topic_regex,
               @subscription
             )

    Enum.each(consumer_pids, fn pid -> refute Process.alive?(pid) end)
  end
end
