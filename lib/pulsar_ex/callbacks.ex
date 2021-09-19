defmodule PulsarEx.ConsumerCallback do
  alias PulsarEx.{Consumer, ConsumerMessage}

  @callback handle_messages([%ConsumerMessage{}], %Consumer.State{}) :: [
              :ok | {:ok, term()} | {:error, term()}
            ]
end

defmodule PulsarEx.WorkerCallback do
  @callback handle_job(job :: atom(), payload :: any()) :: :ok | {:ok, term()} | {:error, term()}
end
