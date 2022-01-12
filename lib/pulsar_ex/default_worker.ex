defmodule PulsarEx.DefaultWorker do
  use PulsarEx.Worker,
    otp_app: :utx,
    subscription: "test",
    topic: "persistent://public/default/test.json",
    subscription_type: :key_shared,
    jobs: [:test]

  @impl true
  def handle_job(job, job_state) do
    IO.inspect(job)
    IO.inspect(job_state)
    :ok
  end
end
