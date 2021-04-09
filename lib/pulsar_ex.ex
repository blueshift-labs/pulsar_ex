defmodule PulsarEx do
  @moduledoc """
  Documentation for `PulsarEx`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> PulsarEx.hello()
      :world

  """

  defdelegate start_consumers(topic, subscription, opts), to: PulsarEx.Application
  defdelegate stop_consumers(topic, subscription), to: PulsarEx.Application

  def produce(topic, payload, options \\ []) do
    :pulserl.produce(topic, payload, options)
  end
end
