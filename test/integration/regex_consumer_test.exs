defmodule PulsarEx.RegexConsumerTest do
  use ExUnit.Case

  alias PulsarEx.{TestRegexConsumer, TestAdmin}

  @topic "persistent://pulsar_ex/IntegrationTest/RegexConsumerTest"
  @topic_regex ~r/^RegexConsumerTest$/

  setup do
    TestRegexConsumer.destroy()
    TestRegexConsumer.setup()
    TestAdmin.reset_subscription(@topic, TestRegexConsumer.subscription())

    {:ok, []}
  end

  @tag timeout: :infinity
  test "regex consumer discovers an existing matching topic and consumes messages" do
    for i <- 1..10 do
      assert {:ok, _} = PulsarEx.Cluster.produce("integration", @topic, "message #{i}")
    end

    assert :ok = TestRegexConsumer.start(@topic_regex)

    Process.sleep(10_000)

    assert [received: 10] = TestRegexConsumer.received()
  end
end
