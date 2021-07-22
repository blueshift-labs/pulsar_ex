defmodule PulsarEx.Middlewares.Telemetry do
  @behaviour PulsarEx.Middleware

  alias PulsarEx.JobState

  @impl true
  def call(handler) do
    fn %JobState{job: job, topic: topic, subscription: subscription} = job_state ->
      start = System.monotonic_time()
      metadata = %{job: job, topic: topic, subscription: subscription}
      job_state = handler.(job_state)

      case job_state do
        %JobState{state: :ok} ->
          :telemetry.execute(
            [:pulsar_ex, :handle_job, :success],
            %{count: 1},
            metadata
          )

        %JobState{state: {:ok, _}} ->
          :telemetry.execute(
            [:pulsar_ex, :handle_job, :success],
            %{count: 1},
            metadata
          )

        _ ->
          :telemetry.execute(
            [:pulsar_ex, :handle_job, :error],
            %{count: 1},
            metadata
          )
      end

      :telemetry.execute(
        [:pulsar_ex, :handle_job],
        %{duration: System.monotonic_time() - start},
        metadata
      )

      job_state
    end
  end
end
