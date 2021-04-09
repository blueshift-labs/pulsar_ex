defmodule PulsarEx.ConsumerCallback do
  alias PulsarEx.Pulserl.Structures.ConsumerMessage
  @type state :: map()

  @moduledoc """
  """
  @callback handle_messages([%ConsumerMessage{}], state()) :: [{%ConsumerMessage{}, :ack | :nack}]
end
