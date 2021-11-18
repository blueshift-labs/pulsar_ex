defmodule PulsarEx.DefaultWorker do
  use PulsarEx.Worker,
    otp_app: :utx,
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
