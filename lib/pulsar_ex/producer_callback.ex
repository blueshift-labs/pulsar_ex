defmodule PulsarEx.ProducerCallback do
  @moduledoc """
  This is to make producer mockable for testing
  """
  @type messageId ::
          {:messageId, ledger_id :: integer(), entry_id :: integer(), topic :: String.t(),
           partition :: integer(),
           batch :: {index :: integer(), size :: non_neg_integer()} | :undefined}
  @callback produce(topic :: String.t(), payload :: binary(), opts :: keyword()) ::
              messageId() | {:error, :timeout}
end
