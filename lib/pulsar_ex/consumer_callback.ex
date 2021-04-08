defmodule PulsarEx.ConsumerCallback do
  @type properties :: %{String.t() => String.t()}
  @type state :: map()

  @moduledoc """
  """
  @callback handle_messages(list(), state()) :: [{any(), :ack | :nack}]
end
