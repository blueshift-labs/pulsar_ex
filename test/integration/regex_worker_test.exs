defmodule PulsarEx.RegexWorkerTest do
  use ExUnit.Case

  alias PulsarEx.{TestRegexWorker, TestRegexWorkerDeadLetterConsumer}

  setup do
    TestRegexWorker.destroy()
    TestRegexWorker.setup()

    TestRegexWorker.start()
    TestRegexWorkerDeadLetterConsumer.start()

    {:ok, []}
  end

  @tag timeout: :infinity
  test "regex worker" do
    {pass, fail} =
      Task.async_stream(
        1..1000,
        fn _ ->
          type = [:pass, :fail] |> Enum.random()
          TestRegexWorker.enqueue_job(type, %{test: :rand.uniform(10_000_000)})
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

    assert [pass: ^pass] = TestRegexWorker.passed()
    assert Keyword.get(TestRegexWorker.failed(), :fail) == 4 * fail
    assert [dead_letter: ^fail] = TestRegexWorker.dead_lettered()
  end
end
