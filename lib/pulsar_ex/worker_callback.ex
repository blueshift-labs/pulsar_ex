defmodule PulsarEx.WorkerCallback do
  @callback handle_job(job :: atom(), payload :: any()) :: :ok | {:ok, any()} | {:error, any()}
end
