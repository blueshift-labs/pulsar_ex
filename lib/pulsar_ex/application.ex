defmodule PulsarEx.Application do
  @moduledoc false

  use Application

  alias PulsarEx.{ConnectionSupervisor, ProducerSupervisor, ConsumerSupervisor, SignalHandler}

  @impl true
  def start(_type, _args) do
    SignalHandler.start_link()

    children = [
      ConnectionSupervisor,
      ProducerSupervisor,
      ConsumerSupervisor
    ]

    opts = [strategy: :one_for_one, name: PulsarEx.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
