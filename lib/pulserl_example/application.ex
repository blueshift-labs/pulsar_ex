defmodule PulserlExample.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # {Registry, keys: :unique, name: PulserlExample.ConsumerRegistry, shutdown: :infinity},
      {Registry, keys: :duplicate, name: PulserlExample.ConsumerRegistry, shutdown: :infinity},
      {DynamicSupervisor,
       strategy: :one_for_one,
       name: Neutron.ProducerSupervisor,
       max_restarts: 2_000,
       shutdown: :infinity,
       max_seconds: 3}
    ]

    opts = [strategy: :one_for_one, name: PulserlExample.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def start_consume(topic, consumer_opts \\ []) do
  end
end
