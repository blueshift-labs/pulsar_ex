defmodule PulsarEx.RegexConsumerNewTopicTest do
  use ExUnit.Case

  alias PulsarEx.{TestRegexConsumer, TestAdmin, Admin}

  @topic_initial "persistent://pulsar_ex/IntegrationTest/RegexNewTopicTestInitial"
  @topic_later "persistent://pulsar_ex/IntegrationTest/RegexNewTopicTestLater"
  @topic_regex ~r/^RegexNewTopicTest(Initial|Later)$/

  setup do
    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()
    TestAdmin.reset_subscription(@topic_initial, TestRegexConsumer.subscription())

    {:ok, []}
  end

  @tag timeout: :infinity
  test "periodic refresh discovers a topic created after the regex consumer already started" do
    for i <- 1..10 do
      assert {:ok, _} = PulsarEx.Cluster.produce("integration", @topic_initial, "message #{i}")
    end

    assert :ok = TestRegexConsumer.start(@topic_regex, refresh_interval: 3_000)

    Process.sleep(10_000)

    assert [received: 10] = TestRegexConsumer.received()

    brokers = Application.get_env(:pulsar_ex, :brokers)
    admin_port = Application.get_env(:pulsar_ex, :admin_port)
    assert :ok = Admin.create_topic(brokers, admin_port, @topic_later)
    TestAdmin.reset_subscription(@topic_later, TestRegexConsumer.subscription())

    for i <- 1..10 do
      assert {:ok, _} = PulsarEx.Cluster.produce("integration", @topic_later, "message #{i}")
    end

    Process.sleep(10_000)

    assert [received: 20] = TestRegexConsumer.received()
  end
end
