defmodule PulsarEx.DefaultWorker do
  use PulsarEx.Worker,
    otp_app: :utx,
    subscription_type: :key_shared,
    subscription: :test,
    topic: "persistent://public/default/test1.json"

  @impl true
  def handle_job(job_state) do
    IO.inspect(job_state)

    {:ok, "YES"}
  end
end
