defmodule PulsarEx.Middlewares.Telemetry do
  @behaviour PulsarEx.Middleware

  alias PulsarEx.JobState

  @impl true
  def call(handler) do
    fn %JobState{job: job, topic: topic, subscription: subscription} = job_state ->
      metadata = %{job: job, topic: topic, subscription: subscription}

      :telemetry.span(
        [:pulsar, :handle_job],
        metadata,
        fn ->
          job_state = handler.(job_state)

          case job_state do
            %JobState{state: :ok} ->
              :telemetry.execute(
                [:pulsar, :handle_job, :success, :count],
                %{count: 1},
                metadata
              )

            _ ->
              :telemetry.execute(
                [:pulsar, :handle_job, :error, :count],
                %{count: 1},
                metadata
              )
          end

          {job_state, metadata}
        end
      )
    end
  end
end
