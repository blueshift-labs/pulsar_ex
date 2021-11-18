defmodule PulsarEx.ConsumerCallback do
  alias PulsarEx.{Consumer, ConsumerMessage}

  @callback handle_messages([%ConsumerMessage{}], %Consumer.State{}) :: [
              :ok | {:ok, term()} | {:error, term()}
            ]
end

defmodule PulsarEx.WorkerCallback do
  @callback topic(job :: atom(), params :: any()) :: String.t()
  @callback partition_key(job :: atom(), params :: any()) :: String.t()
  @callback handle_job(job :: atom(), params :: any()) :: :ok | {:ok, term()} | {:error, term()}
end
