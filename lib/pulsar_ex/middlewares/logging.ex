defmodule PulsarEx.Middlewares.Logging do
  @behaviour PulsarEx.Middleware

  alias PulsarEx.JobState

  require Logger

  @impl true
  def call(handler) do
    fn %JobState{job: job} = job_state ->
      started_at = Timex.now()
      Logger.info("start processing job #{job}")

      job_state = handler.(%JobState{job_state | started_at: started_at})

      finished_at = Timex.now()
      duration = Timex.diff(finished_at, started_at, :milliseconds)

      case job_state.state do
        :ok ->
          Logger.info("finished processing job #{job} with duration #{duration}ms")

        state ->
          Logger.error(
            "error processing job #{job} with duration #{duration}ms, #{inspect(state)}",
            payload: job_state.payload
          )
      end

      %JobState{job_state | finished_at: finished_at}
    end
  end
end
