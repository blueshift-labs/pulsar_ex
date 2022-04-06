defmodule PulsarEx.DefaultWorker do
  use PulsarEx.Worker,
    otp_app: :utx,
    subscription: "test",
    topic: "persistent://public/default/test.json",
    subscription_type: :key_shared,
    use_executor: true,
    exec_timeout: 1000,
    inline: false,
    jobs: [:test]

  @impl true
  def handle_job(:test, job_state) do
    Task.async(fn -> IO.inspect(job_state) end) |> Task.await()

    {:ok, "YES"}
  end
end
