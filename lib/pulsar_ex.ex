defmodule PulsarEx do
  @moduledoc """
  PulsarEx is a pure elixir/erlang client for [Apache Pulsar](https://pulsar.apache.org/).
  It's a wrapper of erlang client [pulserl](https://github.com/skulup/pulserl), with a bunch of bug fixes and improvements.
  This wrapper makes it easier to work with from within Elixir
  """

  @doc """
  ## Starts the consumers
  ## Examples
      iex> defmodule ExampleCallback do
      iex>   @behaviour PulsarEx.ConsumerCallback
      iex>
      iex>   def handle_messages(msgs, _state) do
      iex>     msgs |> Enum.map(&{IO.inspect(&1), :ack})
      iex>   end
      iex> end
      iex>
      iex> PulsarEx.start_consumers("test-topic", "test-subscription", ExampleCallback)

  ## Available pulserl options:
  #  consumer_name :: string()
  #  priority_level :: non_neg_integer(),
  #    Only works in shared/failover subscription
  #  properties = [] :: keyword() | %{term() => term()}
  #  initial_position = :latest :: :latest | :earliest
  #  subscription_type = :shared :: :shared | :failover | :exclusive | :key_shared
  #  queue_size = 1000 :: non_neg_integer()
  #  acknowledgments_send_tick = 100 :: non_neg_integer()
  #  max_pending_acknowledgments = 1000 :: non_neg_integer()
  #  queue_refill_threshold :: non_neg_integer(),
  #    When to get next batch of messages from broker, default to half of the queue_size
  #  acknowledgment_timeout = 0 :: non_neg_integer(),
  #    Timeout for sending acknowledgments, 0 means infinity.
  #    If any, must be greater than 1_000(1 second)
  #  nack_message_redelivery_delay = 60000 :: non_neg_integer(),
  #    Redeliver delay for nack'ed messages
  #  dead_letter_topic_name :: string()
  #  dead_letter_topic_max_redeliver_count = 0 :: non_neg_integer(),
  #    Max number of times nack'ed messages can be redelivered0 means infinity

  ## Available PulsarEx options:
  # callback_module,
  #   The module that implements PulsarEx.ConsumerCallback
  # batch_size = 1 :: non_neg_integer(),
  #   Always consume the messages in batch, indicate the size of each batch, defaults to 1
  # poll_interval = 1_000,
  #   How often to poll next batch, defaults to 1 second
  # workers = 1,
  #   Number of workers to work on messages, default to 1.
  #   Note in the case of consuming from partitioned topics and with key_shared subscription,
  #     workers is set to be the same as number of partitions
  """
  def start_consumers(topic, subscription, callback_module),
    do:
      start_consumers(
        topic,
        subscription,
        callback_module,
        Application.get_env(:pulserl, :consumer_opts, [])
      )

  defdelegate start_consumers(topic, subscription, callback_module, opts),
    to: PulsarEx.Application

  defdelegate stop_consumers(topic, subscription), to: PulsarEx.Application

  @produce_timeout 3_000

  @doc """
  ## Synchronously produce message to pulsar, with default producer options
  #  Beging able to asynchronously send messages to pulsar is awesome, but without able to catch the
  #  errors sucks. So this sync version uses the async send, but wait for the batched/async sends to return
  #  with result.
  #
  ## Example Producer Configs:
  ```
     config :pulserl,
       producer_opts: [
         properties: [test: true, env: staging],
         batch_enable: true
       ]
  ```
  ## Examples
      iex> PulsarEx.produce("test-topic", "test-subscription", event_time: :os.system_time(:millisecond))

  ## Default pulserl producer options are picked up from configs:
  #  producer_name :: string()
  #  properties = [] :: keyword() | %{term() => term()}
  #  send_timeout = 30000 :: non_neg_integer()
  #    Timeout sending messages to pulsar, defaults to 30 seconds.
  #    Setting it to 0 means infinity
  #  initial_sequence_id :: non_neg_integer()
  #  batch_enable = true :: boolean()
  #  routing_mode = :round_robin_routing :: :single_routing | :round_robin_routing
  #  block_on_full_queue :: boolean()
  #  batch_max_messages = 1000 :: non_neg_integer()
  #  batch_max_delay_ms = 10 :: non_neg_integer()
  #  max_pending_requests = 50000 :: non_neg_integer()
  #  max_pending_requests_across_partitions = 100000 :: non_neg_integer()

  ## Available PulsarEx options:
  # timeout = 3000,
  #   Timeout value for successfully send out the message to broker
  """
  def produce(topic, payload),
    do: produce(topic, payload, Application.get_env(:pulserl, :producer_opts, []))

  def produce(topic, payload, message_opts) do
    timeout = Keyword.get(message_opts, :timeout, @produce_timeout)
    pid = self()

    :pulserl.produce(
      topic,
      :pulserl_producer.new_message(payload, message_opts),
      &send(pid, &1)
    )

    receive do
      msgID -> msgID
    after
      timeout ->
        {:error, :timeout}
    end
  end
end
