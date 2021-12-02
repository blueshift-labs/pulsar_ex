defmodule PulsarEx.ConsumerCallback do
  alias PulsarEx.{Consumer, ConsumerMessage}

  @callback handle_messages([%ConsumerMessage{}], %Consumer.State{}) :: [
              :ok | {:ok, term()} | {:error, term()}
            ]
end

defmodule PulsarEx.WorkerCallback do
  @callback topic(job :: atom(), params :: any(), message_opts :: keyword()) :: String.t()
  @callback partition_key(job :: atom(), params :: any(), message_opts :: keyword()) :: String.t()
  @callback handle_job(job :: atom(), params :: any()) :: :ok | {:ok, term()} | {:error, term()}
end

defmodule PulsarEx.ProducerCallback do
  alias PulsarEx.Proto.{MessageIdData, ServerError}

  @callback produce(
              topic_name :: String.t(),
              payload :: binary(),
              message_opts :: keyword(),
              producer_opts :: keyword()
            ) :: {:ok, MessageIdData.t()} | {:error, ServerError.t()}
end
