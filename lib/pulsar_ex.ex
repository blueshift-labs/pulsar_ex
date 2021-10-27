defmodule PulsarEx do
  alias PulsarEx.{
    ProducerRegistry,
    ProducerSupervisor,
    ProducerManager,
    PartitionedProducer,
    Topic,
    Partitioner
  }

  @max_attempts 5

  def sync_produce(topic_name, payload, message_opts \\ [], producer_opts \\ []) do
    start = System.monotonic_time()

    reply = retry_produce(true, topic_name, payload, message_opts, producer_opts)

    case reply do
      {:ok, _} ->
        :telemetry.execute(
          [:pulsar_ex, :sync_produce, :success],
          %{count: 1, duration: System.monotonic_time() - start},
          %{topic: topic_name}
        )

      _ ->
        :telemetry.execute(
          [:pulsar_ex, :sync_produce, :error],
          %{count: 1, duration: System.monotonic_time() - start},
          %{topic: topic_name}
        )
    end

    reply
  end

  def async_produce(topic_name, payload, message_opts \\ [], producer_opts \\ []) do
    start = System.monotonic_time()

    reply = retry_produce(false, topic_name, payload, message_opts, producer_opts)

    case reply do
      :ok ->
        :telemetry.execute(
          [:pulsar_ex, :async_produce, :success],
          %{count: 1, duration: System.monotonic_time() - start},
          %{topic: topic_name}
        )

      _ ->
        :telemetry.execute(
          [:pulsar_ex, :async_produce, :error],
          %{count: 1, duration: System.monotonic_time() - start},
          %{topic: topic_name}
        )
    end

    reply
  end

  # in the event of topic rebalancing, producer will take time to reconnect
  defp retry_produce(sync?, topic_name, payload, message_opts, producer_opts, attempts \\ 1) do
    :telemetry.execute(
      [:pulsar_ex, :produce],
      %{attempts: attempts},
      %{topic: topic_name}
    )

    case produce(sync?, topic_name, payload, message_opts, producer_opts) do
      :ok ->
        :ok

      {:ok, message_id} ->
        {:ok, message_id}

      {:error, :producer_not_ready} ->
        :telemetry.execute(
          [:pulsar_ex, :produce, :producer_not_ready],
          %{count: 1},
          %{topic: topic_name}
        )

        if attempts >= @max_attempts do
          {:error, :producer_not_ready}
        else
          Process.sleep(500)
          retry_produce(sync?, topic_name, payload, message_opts, producer_opts, attempts + 1)
        end

      {:error, :closed} ->
        :telemetry.execute(
          [:pulsar_ex, :produce, :closed],
          %{count: 1},
          %{topic: topic_name}
        )

        if attempts >= @max_attempts do
          {:error, :closed}
        else
          Process.sleep(1000)
          retry_produce(sync?, topic_name, payload, message_opts, producer_opts, attempts + 1)
        end

      reply ->
        reply
    end
  end

  defp produce(sync?, topic_name, payload, message_opts, producer_opts)
       when is_map(message_opts) or is_map(producer_opts) do
    produce(sync?, topic_name, payload, Enum.into(message_opts, []), Enum.into(producer_opts, []))
  end

  defp produce(sync?, topic_name, payload, message_opts, producer_opts) do
    case ProducerSupervisor.lookup_partitions(topic_name) do
      [] ->
        case ProducerManager.create(topic_name, producer_opts) do
          :ok ->
            produce(sync?, topic_name, payload, message_opts, producer_opts)

          {:error, :already_started} ->
            produce(sync?, topic_name, payload, message_opts, producer_opts)

          {:error, _} = err ->
            err
        end

      [{_, {%Topic{partition: nil} = topic, partitions}}] ->
        partition_key = Keyword.get(message_opts, :partition_key)
        partition = Partitioner.assign(partition_key, partitions)

        case Registry.lookup(ProducerRegistry, %{topic | partition: partition}) do
          [] ->
            {:error, :producer_not_ready}

          [{pool, _}] ->
            # we are just using the pool to do round robin, producing message doesn't have to block the pool
            producer = :poolboy.checkout(pool)
            :poolboy.checkin(pool, producer)

            start = System.monotonic_time()
            reply = PartitionedProducer.produce(sync?, producer, payload, message_opts)

            :telemetry.execute(
              [:pulsar_ex, :produce, :debug],
              %{duration: System.monotonic_time() - start},
              %{}
            )

            reply
        end
    end
  end
end
