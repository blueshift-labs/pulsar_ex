defmodule PulsarEx.JobState do
  @type t :: %__MODULE__{}

  @derive Jason.Encoder
  @enforce_keys [
    :topic,
    :subscription,
    :job,
    :properties,
    :publish_time,
    :event_time,
    :producer_name,
    :partition_key,
    :ordering_key,
    :deliver_at_time,
    :redelivery_count,
    :payload,
    :state
  ]

  defstruct [
    :topic,
    :subscription,
    :job,
    :properties,
    :publish_time,
    :event_time,
    :producer_name,
    :partition_key,
    :ordering_key,
    :deliver_at_time,
    :redelivery_count,
    :payload,
    :state
  ]
end

defmodule PulsarEx.Middleware do
  alias PulsarEx.JobState

  @type handler :: (JobState.t() -> JobState.t())

  @callback call(handler :: handler) :: handler
end

defmodule PulsarEx.Middlewares.Logging do
  @behaviour PulsarEx.Middleware

  alias PulsarEx.JobState

  require Logger

  @impl true
  def call(handler) do
    fn %JobState{job: job} = job_state ->
      start = System.monotonic_time(:millisecond)
      Logger.debug("start processing job #{job}")

      Logger.debug("processing job #{job} with payload", payload: job_state.payload)

      job_state = handler.(job_state)

      duration = System.monotonic_time(:millisecond) - start

      case job_state.state do
        :ok ->
          Logger.debug("finished processing job #{job} with duration #{duration}ms")

        {:ok, result} ->
          Logger.debug(
            "finished processing job #{job} with duration #{duration}ms, #{inspect(result)}"
          )

        state ->
          Logger.error(
            "error processing job #{job} with duration #{duration}ms, #{inspect(state)}",
            payload: job_state.payload
          )
      end

      job_state
    end
  end
end

defmodule PulsarEx.Middlewares.Telemetry do
  @behaviour PulsarEx.Middleware

  alias PulsarEx.JobState

  @impl true
  def call(handler) do
    fn %JobState{job: job, topic: topic, subscription: subscription} = job_state ->
      start = System.monotonic_time(:millisecond)
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
        %{duration: System.monotonic_time(:millisecond) - start},
        metadata
      )

      job_state
    end
  end
end
