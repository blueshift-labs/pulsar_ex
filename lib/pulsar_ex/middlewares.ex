defmodule PulsarEx.JobState do
  @type t :: %__MODULE__{}

  @enforce_keys [
    :cluster,
    :worker,
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
    :consumer_opts,
    :assigns,
    :state
  ]

  defstruct [
    :cluster,
    :worker,
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
    :consumer_opts,
    :assigns,
    :state
  ]

  def assign(%PulsarEx.JobState{} = job_state, key, value) do
    %{job_state | assigns: Map.put(job_state.assigns, key, value)}
  end
end

defimpl Jason.Encoder, for: PulsarEx.JobState do
  alias PulsarEx.JobState

  @derivable ~w|worker topic subscription job properties publish_time event_time 
    producer_name partition_key ordering_key deliver_at_time redelivery_count 
    payload consumer_opts assigns state|a

  def encode(%JobState{consumer_opts: consumer_opts} = state, opts) do
    consumer_opts = (consumer_opts || []) |> Enum.into(%{})

    state
    |> Map.take(@derivable)
    |> Map.put(:consumer_opts, consumer_opts)
    |> Jason.Encode.map(opts)
  end
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
    fn
      %JobState{job: nil, cluster: cluster} = job_state ->
        start = System.monotonic_time(:millisecond)
        Logger.info("start processing job, on cluster #{cluster}")

        Logger.info("processing job with payload, on cluster #{cluster}",
          payload: job_state.payload
        )

        job_state = handler.(job_state)

        duration = System.monotonic_time(:millisecond) - start

        case job_state.state do
          :ok ->
            Logger.info(
              "finished processing job with duration #{duration}ms, on cluster #{cluster}"
            )

          {:ok, result} ->
            Logger.info(
              "finished processing job with duration #{duration}ms, on cluster #{cluster}, #{inspect(result)}"
            )

          state ->
            Logger.error(
              "error processing job with duration #{duration}ms, on cluster #{cluster}, #{inspect(state)}",
              payload: job_state.payload
            )
        end

        job_state

      %JobState{job: job, cluster: cluster} = job_state ->
        start = System.monotonic_time(:millisecond)
        Logger.info("start processing job #{job}, on cluster #{cluster}")

        Logger.info("processing job #{job} with payload, on cluster #{cluster}",
          payload: job_state.payload
        )

        job_state = handler.(job_state)

        duration = System.monotonic_time(:millisecond) - start

        case job_state.state do
          :ok ->
            Logger.info(
              "finished processing job #{job} with duration #{duration}ms, on cluster #{cluster}"
            )

          {:ok, result} ->
            Logger.info(
              "finished processing job #{job} with duration #{duration}ms, on cluster #{cluster}, #{inspect(result)}"
            )

          state ->
            Logger.error(
              "error processing job #{job} with duration #{duration}ms, on cluster #{cluster}, #{inspect(state)}",
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
    fn %JobState{job: job, topic: topic, subscription: subscription, cluster: cluster} = job_state ->
      start = System.monotonic_time()

      metadata =
        if job do
          %{job: job, topic: topic, subscription: subscription, cluster: cluster}
        else
          %{topic: topic, subscription: subscription, cluster: cluster}
        end

      job_state = handler.(job_state)

      case job_state do
        %JobState{state: :ok} ->
          :telemetry.execute(
            [:pulsar_ex, :worker, :handle_job, :success],
            %{count: 1, duration: System.monotonic_time() - start},
            metadata
          )

        %JobState{state: {:ok, _}} ->
          :telemetry.execute(
            [:pulsar_ex, :worker, :handle_job, :success],
            %{count: 1, duration: System.monotonic_time() - start},
            metadata
          )

        _ ->
          :telemetry.execute(
            [:pulsar_ex, :worker, :handle_job, :error],
            %{count: 1},
            metadata
          )
      end

      job_state
    end
  end
end
