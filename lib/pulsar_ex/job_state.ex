defmodule PulsarEx.JobState do
  @type t :: %__MODULE__{}

  @derive Jason.Encoder
  @enforce_keys [:topic, :subscription, :job, :payload]
  defstruct [:topic, :subscription, :job, :payload, :started_at, :finished_at, :state]
end
