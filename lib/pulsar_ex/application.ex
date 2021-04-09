defmodule PulsarEx.Application do
  @moduledoc false

  use Application
  alias PulsarEx.{Consumer, ConsumerCallback}

  require Logger

  @default_batch_size 1
  @default_poll_interval 1_000
  @default_workers 1
  @default_subscription_type :shared

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :duplicate, name: PulsarEx.ConsumerRegistry, shutdown: :infinity},
      {DynamicSupervisor,
       strategy: :one_for_one,
       name: PulsarEx.ConsumerSupervisor,
       max_restarts: 2_000,
       shutdown: :infinity,
       max_seconds: 3}
    ]

    opts = [strategy: :one_for_one, name: PulsarEx.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def consumers(topic, subscription) do
    Registry.lookup(PulsarEx.ConsumerRegistry, {topic, subscription})
  end

  def start_consumers(topic, subscription, callback_module, opts) do
    behaviours =
      callback_module.module_info[:attributes]
      |> Keyword.pop_values(:behaviour)
      |> elem(0)
      |> List.flatten()

    if !Enum.member?(behaviours, ConsumerCallback) do
      raise "error you need to implement the PulsarEx.ConsumerCallback for your consumer"
    end

    {batch_size, opts} = Keyword.pop(opts, :batch_size, @default_batch_size)
    {poll_interval, opts} = Keyword.pop(opts, :poll_interval, @default_poll_interval)
    {workers, opts} = Keyword.pop(opts, :workers, @default_workers)

    subscription_type = Keyword.get(opts, :subscription_type, @default_subscription_type)

    Logger.info("Starting pulserl consumer",
      topic: topic,
      subscription: subscription,
      opts: opts
    )

    {:ok, consumer} = :pulserl_instance_registry.get_consumer(topic, subscription, opts)

    worker_opts = %{
      batch_size: batch_size,
      poll_interval: poll_interval,
      callback_module: callback_module
    }

    with partitions when %{} != partitions <-
           :pulserl_consumer.get_partitioned_consumers(consumer),
         :key_shared <- subscription_type do
      Logger.info(
        "Subscribing to key_shared partitioned topics, setting workers to be the same as partitions",
        topic: topic,
        subscription: subscription,
        opts: opts
      )

      partitions
      |> Enum.map(
        &Task.async(fn ->
          Logger.info("Starting partitioned consumer worker #{elem(&1, 0)}",
            topic: topic,
            subscription: subscription,
            opts: opts
          )

          DynamicSupervisor.start_child(
            PulsarEx.ConsumerSupervisor,
            {Consumer, [elem(&1, 1), topic, subscription, worker_opts]}
          )
        end)
      )
      |> Task.await_many()
    else
      _ ->
        :lists.seq(1, workers)
        |> Enum.map(
          &Task.async(fn ->
            Logger.info("Starting consumer worker #{&1}",
              topic: topic,
              subscription: subscription,
              opts: opts
            )

            DynamicSupervisor.start_child(
              PulsarEx.ConsumerSupervisor,
              {Consumer, [consumer, topic, subscription, worker_opts]}
            )
          end)
        )
        |> Task.await_many()
    end
  end

  def stop_consumers(topic, subscription) do
    Logger.info("Stopping consumer workers", topic: topic, subscription: subscription)

    consumers(topic, subscription)
    |> Enum.each(&DynamicSupervisor.terminate_child(PulsarEx.ConsumerSupervisor, elem(&1, 0)))

    Logger.info("Stopping pulserl consumer", topic: topic, subscription: subscription)
    {:ok, pid} = :pulserl_instance_registry.get_consumer(topic, subscription, [])
    :pulserl_consumer.close(pid)
  end
end
