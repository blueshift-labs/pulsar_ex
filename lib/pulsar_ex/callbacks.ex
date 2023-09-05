defmodule PulsarEx.ConsumerCallback do
  alias PulsarEx.{Consumer, ConsumerMessage}

  @callback handle_messages([%ConsumerMessage{}], %Consumer{}) :: [
              :ok | {:ok, term()} | {:error, term()}
            ]

  @callback send_to_dead_letter(%ConsumerMessage{}, %Consumer{}) ::
              :ok | {:ok, term()} | {:error, term()}
end

defmodule PulsarEx.WorkerCallback do
  alias PulsarEx.JobState

  @callback cluster(params :: any(), message_opts :: keyword()) ::
              keyword() | String.t()
  @callback cluster(job :: atom(), params :: any(), message_opts :: keyword()) ::
              keyword() | String.t()

  @callback topic(params :: any(), message_opts :: keyword()) :: String.t()
  @callback topic(job :: atom(), params :: any(), message_opts :: keyword()) :: String.t()

  @callback partition_key(params :: any(), message_opts :: keyword()) :: String.t()
  @callback partition_key(job :: atom(), params :: any(), message_opts :: keyword()) :: String.t()

  @callback ordering_key(params :: any(), message_opts :: keyword()) :: String.t()
  @callback ordering_key(job :: atom(), params :: any(), message_opts :: keyword()) :: String.t()

  @callback handle_job(job_state :: JobState.t()) :: :ok | {:ok, term()} | {:error, term()}
  @callback handle_job(job :: atom(), job_state :: JobState.t()) ::
              :ok | {:ok, term()} | {:error, term()}
end

defmodule PulsarEx.ProducerCallback do
  alias PulsarEx.Proto.MessageIdData

  @callback produce(
              cluster_name :: atom() | String.t(),
              topic_name :: String.t(),
              payload :: binary(),
              message_opts :: keyword(),
              producer_opts :: keyword()
            ) :: {:ok, MessageIdData.t()} | {:error, term()}
end
