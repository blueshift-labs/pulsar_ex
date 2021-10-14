defmodule PulsarEx.PartitionedProducerManager do
  defmodule State do
    @enforce_keys [
      :sup,
      :brokers,
      :admin_port,
      :topic_name,
      :topic,
      :producer_opts,
      :lookup,
      :refresh_interval
    ]
    defstruct [
      :sup,
      :brokers,
      :admin_port,
      :topic_name,
      :topic,
      :producer_opts,
      :lookup,
      :refresh_interval
    ]
  end

  use GenServer

  require Logger

  alias PulsarEx.{Topic, Admin, ProducerRegistry, PartitionedProducer}

  @refresh_interval 60_000
  @num_producers 1

  def start_link({topic_name, producer_opts, lookup, sup}) do
    GenServer.start_link(__MODULE__, {topic_name, producer_opts, lookup, sup})
  end

  @impl true
  def init({topic_name, producer_opts, lookup, sup}) do
    Logger.debug("Starting partition producer manager for topic #{topic_name}")

    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    with {:ok, %Topic{partition: nil} = topic} <- Topic.parse(topic_name),
         {:ok, partitions} <- Admin.lookup_topic_partitions(brokers, admin_port, topic) do
      refresh_interval =
        max(Keyword.get(producer_opts, :refresh_interval, @refresh_interval), 5_000)

      if partitions > 0 do
        Process.send_after(self(), :refresh, refresh_interval + :rand.uniform(refresh_interval))

        result =
          0..(partitions - 1)
          |> Task.async_stream(
            fn partition ->
              DynamicSupervisor.start_child(
                sup,
                pool_spec(%{topic | partition: partition}, producer_opts)
              )
            end,
            max_concurrency: 8
          )
          |> Enum.reduce_while(:ok, fn
            {:ok, {:ok, _}}, :ok -> {:cont, :ok}
            {:ok, {:error, _} = err}, :ok -> {:halt, err}
            {:exit, err}, :ok -> {:halt, err}
          end)

        case result do
          :ok ->
            :ets.insert(lookup, {topic_name, {topic, partitions}})

            state = %State{
              sup: sup,
              brokers: brokers,
              admin_port: admin_port,
              topic_name: topic_name,
              topic: topic,
              producer_opts: producer_opts,
              lookup: lookup,
              refresh_interval: refresh_interval
            }

            {:ok, state}

          _ ->
            {:stop, result}
        end
      else
        DynamicSupervisor.start_child(sup, pool_spec(topic, producer_opts))
        |> case do
          {:ok, _} ->
            :ets.insert(lookup, {topic_name, {topic, partitions}})

            state = %State{
              sup: sup,
              brokers: brokers,
              admin_port: admin_port,
              topic_name: topic_name,
              topic: topic,
              producer_opts: producer_opts,
              lookup: lookup,
              refresh_interval: refresh_interval
            }

            {:ok, state}

          {:error, _} = err ->
            {:stop, err}
        end
      end
    else
      {:ok, %Topic{}} -> {:stop, {:error, :no_direct_produce}}
      {:error, _} = err -> {:stop, err}
    end
  end

  @impl true
  def handle_info(
        :refresh,
        %{
          sup: sup,
          topic: topic,
          topic_name: topic_name,
          producer_opts: producer_opts,
          lookup: lookup
        } = state
      ) do
    with {:ok, partitions} <-
           Admin.lookup_topic_partitions(state.brokers, state.admin_port, topic) do
      [{_, {_, prev_partitions}}] = :ets.lookup(lookup, topic_name)

      Process.send_after(
        self(),
        :refresh,
        state.refresh_interval + :rand.uniform(state.refresh_interval)
      )

      cond do
        prev_partitions == partitions ->
          Logger.debug("No changes to partitions for topic #{topic_name}")

          {:noreply, state}

        prev_partitions < partitions ->
          Logger.warn("Detected increased partitions for topic #{topic_name}")

          err =
            prev_partitions..(partitions - 1)
            |> Enum.reduce_while(nil, fn partition, nil ->
              DynamicSupervisor.start_child(
                sup,
                pool_spec(%{topic | partition: partition}, producer_opts)
              )
              |> case do
                {:ok, _} -> {:cont, nil}
                {:error, _} = err -> {:halt, err}
              end
            end)

          case err do
            nil ->
              :ets.insert(lookup, {topic_name, {topic, partitions}})

              {:noreply, state}

            _ ->
              {:stop, err, state}
          end

        prev_partitions > partitions ->
          Logger.warn("Detected decreased partitions for topic #{topic_name}")

          :ets.insert(lookup, {topic_name, {topic, partitions}})

          partitions..(prev_partitions - 1)
          |> Enum.each(fn partition ->
            case Registry.lookup(ProducerRegistry, %{topic | partition: partition}) do
              [] -> nil
              [{pid, _}] -> DynamicSupervisor.terminate_child(sup, pid)
            end
          end)

          {:noreply, state}
      end
    else
      {:error, err} ->
        Logger.error("Error refreshing partitions for topic #{topic_name}, #{inspect(err)}")
        {:stop, {:error, err}, state}
    end
  end

  defp pool_spec(%Topic{} = topic, producer_opts) do
    {
      Topic.to_name(topic),
      {
        :poolboy,
        :start_link,
        [
          [
            name: {:via, Registry, {ProducerRegistry, topic}},
            worker_module: PartitionedProducer,
            size: Keyword.get(producer_opts, :num_producers, @num_producers),
            max_overflow: 0,
            strategy: :fifo
          ],
          {topic, producer_opts}
        ]
      },
      :permanent,
      :infinity,
      :supervisor,
      [:poolboy, PartitionedProducer]
    }
  end
end
