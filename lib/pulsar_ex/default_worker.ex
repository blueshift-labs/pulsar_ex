defmodule PulsarEx.DefaultWorker do
  use PulsarEx.Worker,
    otp_app: :utx,
    subscription_type: :shared,
    subscription: :test,
    topic: "persistent://public/default/test"

  @impl true
  def handle_job(job_state) do
    IO.inspect(job_state.payload)

    {:ok, "YES"}
  end
end
