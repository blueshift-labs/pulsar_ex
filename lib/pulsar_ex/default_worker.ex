defmodule PulsarEx.DefaultWorker do
  use PulsarEx.Worker,
    otp_app: :utx,
    subscription_type: :key_shared,
    use_executor: true,
    exec_timeout: 1000,
    inline: false

  @impl true
  def handle_job(job_state) do
    IO.inspect(job_state)

    {:ok, "YES"}
  end
end
