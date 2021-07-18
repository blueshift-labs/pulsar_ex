defmodule PulsarEx.WorkerCallback do
  @callback handle_job(job :: atom(), payload :: any()) :: :ok | {:error, any()}
end
