defmodule PulsarEx.ConsumerCallback do
  alias PulsarEx.Pulserl.Structures.ConsumerMessage
  @type state :: map()

  @moduledoc """
  Always handle messages in batch. If the batch_size is set to 1, then only up to 1 message will be handled.
  If batch_size is n, then handle_messages will handle 0 < messages <= n.

  Expect the return of a list of [{:ack | :nack, msg}], as the consumer genserver will use it to ack back to pulsar
  """
  @callback handle_messages([%ConsumerMessage{}], state()) :: [{:ack | :nack, %ConsumerMessage{}}]
end
