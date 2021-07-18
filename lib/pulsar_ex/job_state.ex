defmodule PulsarEx.JobState do
  @type t :: %__MODULE__{}
  defstruct [:topic, :subscription, :job, :payload, :started_at, :finished_at, :state]
end
