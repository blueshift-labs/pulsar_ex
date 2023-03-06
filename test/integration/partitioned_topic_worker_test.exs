defmodule PulsarEx.PartitionedTopicWorkerTest do
  use ExUnit.Case

  alias PulsarEx.{
    TestPartitionedTopicWorker,
    TestPartitionedTopicDeadLetterConsumer
  }

  setup do
    TestPartitionedTopicWorker.destroy()
    TestPartitionedTopicWorker.setup()

    TestPartitionedTopicWorker.start()
    TestPartitionedTopicDeadLetterConsumer.start()

    {:ok, []}
  end

  @tag timeout: :infinity
  test "partitioned topic worker" do
    {pass, fail} =
      Task.async_stream(
        1..1000,
        fn _ ->
          type = [:pass, :fail] |> Enum.random()
          TestPartitionedTopicWorker.enqueue_job(type, %{test: :rand.uniform(10_000_000)})
          type
        end,
        max_concurrency: 16,
        timeout: 30_000
      )
      |> Enum.to_list()
      |> Enum.split_with(&(&1 == {:ok, :pass}))

    pass = Enum.count(pass)
    fail = Enum.count(fail)

    Process.sleep(120_000)

    assert [pass: ^pass] = TestPartitionedTopicWorker.passed()
    assert Keyword.get(TestPartitionedTopicWorker.failed(), :fail) == 6 * fail
    assert [dead_letter: ^fail] = TestPartitionedTopicWorker.dead_lettered()
  end
end
