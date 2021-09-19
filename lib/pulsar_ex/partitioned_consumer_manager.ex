defmodule PulsarEx.PartitionedConsumerManager do
  defmodule State do
    @enforce_keys [
      :sup,
      :brokers,
      :admin_port,
      :topic_name,
      :topic,
      :subscription,
      :module,
      :consumer_opts,
      :lookup,
      :refresh_interval
    ]
    defstruct [
      :sup,
      :brokers,
      :admin_port,
      :topic_name,
      :topic,
      :subscription,
      :module,
      :consumer_opts,
      :lookup,
      :refresh_interval
    ]
  end

  use GenServer

  require Logger

  alias PulsarEx.{Topic, Admin, ConsumerRegistry}

  @refresh_interval 30_000
  @num_consumers 1

  def start_link({topic_name, subscription, module, consumer_opts, lookup, sup}) do
    GenServer.start_link(
      __MODULE__,
      {topic_name, subscription, module, consumer_opts, lookup, sup}
    )
  end

  @impl true
  def init({topic_name, subscription, module, consumer_opts, lookup, sup}) do
    Logger.debug(
      "Starting partition consumer manager for topic #{topic_name} with subscription #{
        subscription
      }"
    )

    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    with {:ok, %Topic{} = topic} <- Topic.parse(topic_name),
         {:ok, partitions} <- Admin.lookup_topic_partitions(brokers, admin_port, topic) do
      refresh_interval =
        max(Keyword.get(consumer_opts, :refresh_interval, @refresh_interval), 5_000)

      if partitions > 0 do
        Process.send_after(self(), :refresh, refresh_interval + :rand.uniform(refresh_interval))

        err =
          0..(partitions - 1)
          |> Enum.reduce_while(nil, fn partition, nil ->
            DynamicSupervisor.start_child(
              sup,
              pool_spec(%{topic | partition: partition}, subscription, module, consumer_opts)
            )
            |> case do
              {:ok, _} -> {:cont, nil}
              {:error, _} = err -> {:halt, err}
            end
          end)

        case err do
          nil ->
            :ets.insert(lookup, {topic_name, {topic, partitions}})

            state = %State{
              sup: sup,
              brokers: brokers,
              admin_port: admin_port,
              topic_name: topic_name,
              topic: topic,
              subscription: subscription,
              module: module,
              consumer_opts: consumer_opts,
              lookup: lookup,
              refresh_interval: refresh_interval
            }

            {:ok, state}

          _ ->
            {:stop, err}
        end
      else
        DynamicSupervisor.start_child(sup, pool_spec(topic, subscription, module, consumer_opts))
        |> case do
          {:ok, _} ->
            :ets.insert(lookup, {topic_name, {topic, partitions}})

            state = %State{
              sup: sup,
              brokers: brokers,
              admin_port: admin_port,
              topic_name: topic_name,
              topic: topic,
              subscription: subscription,
              module: module,
              consumer_opts: consumer_opts,
              lookup: lookup,
              refresh_interval: refresh_interval
            }

            {:ok, state}

          {:error, _} = err ->
            {:stop, err}
        end
      end
    else
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
          subscription: subscription,
          module: module,
          consumer_opts: consumer_opts,
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
                pool_spec(%{topic | partition: partition}, subscription, module, consumer_opts)
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
            case Registry.lookup(
                   ConsumerRegistry,
                   {%{topic | partition: partition}, subscription}
                 ) do
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

  defp pool_spec(%Topic{} = topic, subscription, module, consumer_opts) do
    {
      {Topic.to_name(topic), subscription},
      {
        :poolboy,
        :start_link,
        [
          [
            name: {:via, Registry, {ConsumerRegistry, {topic, subscription}}},
            worker_module: module,
            size: Keyword.get(consumer_opts, :num_consumers, @num_consumers),
            max_overflow: 0,
            strategy: :fifo
          ],
          {topic, subscription, consumer_opts}
        ]
      },
      :permanent,
      :infinity,
      :supervisor,
      [:poolboy, module]
    }
  end
end
