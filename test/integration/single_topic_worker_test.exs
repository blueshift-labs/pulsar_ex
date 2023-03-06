defmodule PulsarEx.SimpleTopicWorkerTest do
  use ExUnit.Case

  alias PulsarEx.{TestSimpleTopicWorker, TestSimpleTopicDeadLetterConsumer}

  setup do
    TestSimpleTopicWorker.destroy()
    TestSimpleTopicWorker.setup()

    TestSimpleTopicWorker.start()
    TestSimpleTopicDeadLetterConsumer.start()

    {:ok, []}
  end

  @tag timeout: :infinity
  test "simple topic worker" do
    {pass, fail} =
      Task.async_stream(
        1..1000,
        fn _ ->
          type = [:pass, :fail] |> Enum.random()
          TestSimpleTopicWorker.enqueue_job(type, %{test: :rand.uniform(10_000_000)})
          type
        end,
        max_concurrency: 16,
        timeout: 30_000
      )
      |> Enum.to_list()
      |> Enum.split_with(&(&1 == {:ok, :pass}))

    pass = Enum.count(pass)
    fail = Enum.count(fail)

    Process.sleep(60000)

    assert [pass: ^pass] = TestSimpleTopicWorker.passed()
    assert Keyword.get(TestSimpleTopicWorker.failed(), :fail) == 4 * fail
    assert [dead_letter: ^fail] = TestSimpleTopicWorker.dead_lettered()
  end
end
