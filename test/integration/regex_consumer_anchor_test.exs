defmodule PulsarEx.RegexConsumerAnchorTest do
  use ExUnit.Case

  alias PulsarEx.{TestRegexConsumer, TestAdmin}

  @topic "persistent://pulsar_ex/IntegrationTest/RegexAnchorTest"
  @topic_suffix "persistent://pulsar_ex/IntegrationTest/RegexAnchorTest.suffix"
  @topic_regex ~r/^RegexAnchorTest$/

  setup do
    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()
    TestAdmin.reset_subscription(@topic, TestRegexConsumer.subscription())

    {:ok, []}
  end

  @tag timeout: :infinity
  test "anchored regex consumes the matching topic and ignores a topic an unanchored pattern would also match" do
    for i <- 1..10 do
      assert {:ok, _} = PulsarEx.Cluster.produce("integration", @topic, "message #{i}")
    end

    for i <- 1..5 do
      assert {:ok, _} = PulsarEx.Cluster.produce("integration", @topic_suffix, "message #{i}")
    end

    assert :ok = TestRegexConsumer.start(@topic_regex)

    Process.sleep(10_000)

    assert [received: 10] = TestRegexConsumer.received()
  end
end
