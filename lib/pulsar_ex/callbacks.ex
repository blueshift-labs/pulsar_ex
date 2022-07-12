defmodule PulsarEx.ConsumerCallback do
  alias PulsarEx.{Consumer, ConsumerMessage}

  @callback handle_messages([%ConsumerMessage{}], %Consumer.State{}) :: [
              :ok | {:ok, term()} | {:error, term()}
            ]
end

defmodule PulsarEx.WorkerCallback do
  alias PulsarEx.JobState

  @callback topic(job :: atom(), params :: any(), message_opts :: keyword()) :: String.t()
  @callback partition_key(job :: atom(), params :: any(), message_opts :: keyword()) :: String.t()
  @callback ordering_key(job :: atom(), params :: any(), message_opts :: keyword()) :: String.t()
  @callback handle_job(job :: atom(), job_state :: JobState.t()) ::
              :ok | {:ok, term()} | {:error, term()}
end

defmodule PulsarEx.ProducerCallback do
  alias PulsarEx.Proto.{MessageIdData}

  @callback produce(
              cluster :: atom(),
              topic_name :: String.t(),
              payload :: binary(),
              message_opts :: keyword(),
              producer_opts :: keyword()
            ) :: {:ok, MessageIdData.t()} | {:error, term()}

  @callback async_produce(
              cluster :: atom(),
              topic_name :: String.t(),
              payload :: binary(),
              message_opts :: keyword(),
              producer_opts :: keyword()
            ) :: :ok | {:error, term()}
end
