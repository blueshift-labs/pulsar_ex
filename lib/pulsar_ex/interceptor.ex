defmodule PulsarEx.JobInfo do
  @type t :: %__MODULE__{}

  @enforce_keys [
    :cluster,
    :worker,
    :topic,
    :job,
    :params,
    :partition_key,
    :ordering_key,
    :message_opts,
    :assigns,
    :state
  ]

  defstruct [
    :cluster,
    :worker,
    :topic,
    :job,
    :params,
    :partition_key,
    :ordering_key,
    :message_opts,
    :assigns,
    :state
  ]

  def assign(%PulsarEx.JobInfo{} = job_info, key, value) do
    %{job_info | assigns: Map.put(job_info.assigns, key, value)}
  end
end

defimpl Jason.Encoder, for: PulsarEx.JobInfo do
  alias PulsarEx.JobInfo

  @derivable ~w|cluster worker topic job params partition_key ordering_key message_opts assigns state|a

  def encode(%JobInfo{message_opts: message_opts} = job_info, opts) do
    message_opts = (message_opts || []) |> Enum.into(%{})

    job_info
    |> Map.take(@derivable)
    |> Map.put(:message_opts, message_opts)
    |> Jason.Encode.map(opts)
  end
end

defmodule PulsarEx.Interceptor do
  alias PulsarEx.JobInfo

  @type handler :: (JobInfo.t() -> JobInfo.t())

  @callback call(handler :: handler) :: handler
end
