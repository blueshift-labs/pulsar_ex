defmodule PulsarEx.TestRegexWorker do
  use PulsarEx.Worker,
    otp_app: :pulsar_ex,
    cluster: "integration",
    tenant: "pulsar_ex",
    namespace: "IntegrationTest",
    topic: ~r/^RegexWorkerTest$/,
    dead_letter_topic: "persistent://pulsar_ex/IntegrationTest/RegexWorkerTest.dead_letters",
    subscription: :regex_worker_test,
    subscription_type: :shared,
    max_redelivery_attempts: 3,
    jobs: [:pass, :fail]

  @impl true
  def topic(_job, _params, _message_opts) do
    "persistent://pulsar_ex/IntegrationTest/RegexWorkerTest"
  end

  @impl true
  def handle_job(:pass, _job_state) do
    :ets.update_counter(:regex_worker_test, :pass, {2, 1}, {:pass, 0})

    {:ok, "YES"}
  end

  def handle_job(:fail, _job_state) do
    :ets.update_counter(:regex_worker_test, :fail, {2, 1}, {:fail, 0})

    {:error, :fail}
  end

  def setup() do
    unless Enum.member?(:ets.all(), :regex_worker_test) do
      :ets.new(:regex_worker_test, [
        :named_table,
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])
    end
  end

  def destroy() do
    if Enum.member?(:ets.all(), :regex_worker_test) do
      :ets.delete(:regex_worker_test)
    end
  end

  def passed() do
    :ets.lookup(:regex_worker_test, :pass)
  end

  def failed() do
    :ets.lookup(:regex_worker_test, :fail)
  end

  def dead_lettered() do
    :ets.lookup(:regex_worker_test, :dead_letter)
  end
end

defmodule PulsarEx.TestRegexWorkerDeadLetterConsumer do
  use PulsarEx.Consumer

  def start() do
    PulsarEx.Cluster.start_consumer(
      "integration",
      "persistent://pulsar_ex/IntegrationTest/RegexWorkerTest.dead_letters",
      "test",
      __MODULE__,
      []
    )
  end

  @impl true
  def handle_messages(messages, _state) do
    messages
    |> Enum.map(fn _message ->
      :ets.update_counter(:regex_worker_test, :dead_letter, {2, 1}, {:dead_letter, 0})

      :ok
    end)
  end
end
