defmodule PulsarEx.DefaultWorker do
  use PulsarEx.Worker,
    otp_app: :utx,
    topic: "persistent://public/default/1.json",
    subscription: "test",
    subscription_type: :failover,
    jobs: [:test]

  @impl true
  def handle_job(job, payload) do
    IO.inspect(job)
    IO.inspect(payload)
    :ok
  end
end
