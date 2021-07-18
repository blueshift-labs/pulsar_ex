defmodule PulsarEx.Middleware do
  alias PulsarEx.JobState

  @type handler :: (JobState.t() -> JobState.t())

  @callback call(handler :: handler) :: handler
end
