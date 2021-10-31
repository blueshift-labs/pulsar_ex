defmodule PulsarEx.Consumer do
  defmodule State do
    @enforce_keys [
      :state,
      :topic,
      :topic_name,
      :topic_logical_name,
      :subscription,
      :brokers,
      :admin_port,
      :consumer_id,
      :subscription_type,
      :connection_attempt,
      :max_connection_attempts,
      :receiving_queue_size,
      :refill_queue_size_watermark,
      :flow_permits_watermark,
      :batch_size,
      :redelivery_policy,
      :max_redelivery_attempts,
      :dead_letter_topic,
      :poll_interval,
      :ack_interval,
      :redelivery_interval,
      :dead_letter_producer_opts,
      :permits,
      :queue,
      :queue_size,
      :batch,
      :acks,
      :consumer_opts,
      :metadata
    ]
    defstruct [
      :state,
      :topic,
      :topic_name,
      :topic_logical_name,
      :subscription,
      :brokers,
      :admin_port,
      :consumer_id,
      :subscription_type,
      :connection_attempt,
      :max_connection_attempts,
      :receiving_queue_size,
      :refill_queue_size_watermark,
      :flow_permits_watermark,
      :batch_size,
      :redelivery_policy,
      :max_redelivery_attempts,
      :dead_letter_topic,
      :poll_interval,
      :ack_interval,
      :redelivery_interval,
      :dead_letter_producer_opts,
      :permits,
      :queue,
      :queue_size,
      :batch,
      :acks,
      :consumer_opts,
      :metadata,
      :broker,
      :priority_level,
      :read_compacted,
      :initial_position,
      :consumer_name,
      :properties,
      :connection,
      :connection_ref
    ]
  end

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      alias PulsarEx.{
        Topic,
        Admin,
        Bitset,
        ConnectionManager,
        Connection,
        ConsumerCallback,
        ConsumerRegistry
      }

      alias PulsarEx.Consumer.State

      require Logger

      use GenServer
      @behaviour ConsumerCallback

      @subscription_type Keyword.get(opts, :subscription_type, :shared)
      @receiving_queue_size Keyword.get(opts, :receiving_queue_size, 100)
      @flow_control_watermark Keyword.get(opts, :flow_control_watermark, 0.5)
      @batch_size Keyword.get(opts, :batch_size, 1)
      @redelivery_policy Keyword.get(opts, :redelivery_policy, :exp)
      @max_redelivery_attempts Keyword.get(opts, :max_redelivery_attempts, 3)
      @dead_letter_topic Keyword.get(opts, :dead_letter_topic, nil)
      @poll_interval Keyword.get(opts, :poll_interval, 50)
      @ack_interval Keyword.get(opts, :ack_interval, 1000)
      @redelivery_interval Keyword.get(opts, :redelivery_interval, 1000)
      @dead_letter_producer_opts Keyword.get(opts, :dead_letter_producer_opts,
                                   batch_enabled: true,
                                   batch_size: 100,
                                   flush_interval: 1000
                                 )
      @max_connection_attempts Keyword.get(opts, :max_connection_attempts, 5)
      @connection_interval 1000

      def start_link({topic_name, partition, subscription, consumer_opts}) do
        GenServer.start_link(__MODULE__, {topic_name, partition, subscription, consumer_opts})
      end

      @impl true
      def init({topic_name, nil, subscription, consumer_opts}) do
        case Topic.parse(topic_name) do
          {:ok, %Topic{} = topic} -> init({topic, subscription, consumer_opts})
          err -> {:stop, err}
        end
      end

      @impl true
      def init({topic_name, partition, subscription, consumer_opts}) do
        case Topic.parse(topic_name) do
          {:ok, %Topic{} = topic} ->
            init({%{topic | partition: partition}, subscription, consumer_opts})

          err ->
            {:stop, err}
        end
      end

      @impl true
      def init({%Topic{} = topic, subscription, consumer_opts}) do
        Process.flag(:trap_exit, true)

        topic_name = Topic.to_name(topic)

        Logger.debug(
          "Starting consumer for topic #{topic_name} with subscription #{subscription}"
        )

        topic_logical_name = Topic.to_logical_name(topic)

        metadata =
          if topic.partition == nil do
            %{topic: topic_logical_name, subscription: subscription}
          else
            %{topic: topic_logical_name, partition: topic.partition, subscription: subscription}
          end

        brokers = Application.fetch_env!(:pulsar_ex, :brokers)
        admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

        consumer_id = PulsarEx.Application.consumer_id()

        subscription_type = Keyword.get(consumer_opts, :subscription_type, @subscription_type)

        receiving_queue_size =
          max(Keyword.get(consumer_opts, :receiving_queue_size, @receiving_queue_size), 2)

        flow_control_watermark =
          Keyword.get(consumer_opts, :flow_control_watermark, @flow_control_watermark)

        refill_queue_size_watermark =
          min(
            max(trunc(receiving_queue_size * flow_control_watermark), 1),
            receiving_queue_size
          )

        flow_permits_watermark = receiving_queue_size - refill_queue_size_watermark

        batch_size =
          min(
            max(Keyword.get(consumer_opts, :batch_size, @batch_size), 1),
            receiving_queue_size
          )

        max_redelivery_attempts =
          max(Keyword.get(consumer_opts, :max_redelivery_attempts, @max_redelivery_attempts), 1)

        redelivery_policy = Keyword.get(consumer_opts, :redelivery_policy, @redelivery_policy)
        dead_letter_topic = Keyword.get(consumer_opts, :dead_letter_topic, @dead_letter_topic)
        poll_interval = max(Keyword.get(consumer_opts, :poll_interval, @poll_interval), 10)

        ack_interval = max(Keyword.get(consumer_opts, :ack_interval, @ack_interval), 1_000)

        redelivery_interval =
          max(Keyword.get(consumer_opts, :redelivery_interval, @redelivery_interval), 1_000)

        dead_letter_producer_opts =
          Keyword.get(consumer_opts, :dead_letter_producer_opts, @dead_letter_producer_opts)

        max_connection_attempts =
          min(Keyword.get(consumer_opts, :max_connection_attempts, @max_connection_attempts), 10)

        state = %State{
          state: :connecting,
          topic: topic,
          topic_name: topic_name,
          topic_logical_name: topic_logical_name,
          subscription: subscription,
          consumer_id: consumer_id,
          brokers: brokers,
          admin_port: admin_port,
          subscription_type: subscription_type,
          receiving_queue_size: receiving_queue_size,
          refill_queue_size_watermark: refill_queue_size_watermark,
          flow_permits_watermark: flow_permits_watermark,
          batch_size: batch_size,
          redelivery_policy: redelivery_policy,
          max_redelivery_attempts: max_redelivery_attempts,
          dead_letter_topic: dead_letter_topic,
          poll_interval: poll_interval,
          ack_interval: ack_interval,
          redelivery_interval: redelivery_interval,
          dead_letter_producer_opts: dead_letter_producer_opts,
          permits: receiving_queue_size,
          consumer_opts: consumer_opts,
          metadata: metadata,
          max_connection_attempts: max_connection_attempts,
          connection_attempt: 0,
          queue: :queue.new(),
          queue_size: 0,
          batch: [],
          acks: %{}
        }

        Process.send(self(), :connect, [])

        Process.send(self(), :poll, [])
        Process.send_after(self(), :acks, state.ack_interval)
        Process.send_after(self(), :nacks, state.redelivery_interval)

        {:ok, state}
      end

      @impl true
      def handle_info(:connect, %{state: :connecting} = state) do
        if state.connection_ref != nil do
          Process.demonitor(state.connection_ref)
        end

        state = %{state | connection_ref: nil}

        with {:ok, broker} <- Admin.lookup_topic(state.brokers, state.admin_port, state.topic),
             {:ok, connection} <- ConnectionManager.get_connection(broker),
             {:ok, reply} <-
               Connection.subscribe(
                 connection,
                 state.consumer_id,
                 state.topic_name,
                 state.subscription,
                 state.subscription_type,
                 state.consumer_opts
               ) do
          %{
            priority_level: priority_level,
            read_compacted: read_compacted,
            initial_position: initial_position,
            consumer_name: consumer_name,
            subscription_type: subscription_type,
            properties: properties
          } = reply

          ref = Process.monitor(connection)

          metadata =
            properties
            |> Enum.into(%{}, fn {k, v} -> {String.to_atom(k), v} end)
            |> Map.merge(state.metadata)

          state = %{
            state
            | state: :ready,
              broker: broker,
              priority_level: priority_level,
              read_compacted: read_compacted,
              initial_position: initial_position,
              consumer_name: consumer_name,
              subscription_type: subscription_type,
              properties: properties,
              connection: connection,
              connection_ref: ref,
              metadata: metadata,
              connection_attempt: 0,
              queue: :queue.new(),
              queue_size: 0,
              batch: [],
              permits: state.receiving_queue_size
          }

          Logger.debug(
            "Subscribed consumer for topic #{state.topic_name} with subscription #{
              state.subscription
            }"
          )

          :telemetry.execute(
            [:pulsar_ex, :consumer, :connect, :success],
            %{count: 1},
            state.metadata
          )

          {:noreply, state}
        else
          err ->
            Logger.debug(
              "Error subscribing consumer for topic #{state.topic_name} with subscription #{
                state.subscription
              }, #{inspect(err)}"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :connect, :error],
              %{count: 1},
              state.metadata
            )

            state = %{state | connection_attempt: state.connection_attempt + 1}

            if state.connection_attempt < state.max_connection_attempts do
              Process.send_after(
                self(),
                :connect,
                @connection_interval * state.connection_attempt
              )

              {:noreply, state}
            else
              {:stop, err, state}
            end
        end
      end

      @impl true
      def handle_info({:DOWN, _, _, _, _}, state) do
        Logger.error("Connection down for consumer with topic #{state.topic_name}")

        {:stop, {:error, :connection_down}, state}
      end

      @impl true
      def handle_info(:acks, %{state: :connecting} = state) do
        Process.send_after(self(), :acks, state.ack_interval)
        {:noreply, state}
      end

      @impl true
      def handle_info(:acks, state) do
        {available_acks, acks} = Enum.split_with(state.acks, &match?({_, true}, &1))

        cond do
          length(available_acks) == 0 ->
            Process.send_after(self(), :acks, state.ack_interval)
            {:noreply, state}

          true ->
            start = System.monotonic_time(:millisecond)

            available_acks =
              Enum.map(available_acks, fn {{ledgerId, entryId}, _} ->
                {ledgerId, entryId}
              end)

            case Connection.ack(state.connection, state.consumer_id, :individual, available_acks) do
              :ok ->
                Logger.debug(
                  "Sent #{length(available_acks)} acks from consumer #{state.consumer_id} for topic #{
                    state.topic_name
                  }"
                )

                :telemetry.execute(
                  [:pulsar_ex, :consumer, :ack, :success],
                  %{
                    count: 1,
                    acks: length(available_acks),
                    duration: System.monotonic_time(:millisecond) - start
                  },
                  state.metadata
                )

                Process.send_after(self(), :acks, state.ack_interval)
                {:noreply, %{state | acks: Enum.into(acks, %{})}}

              {:error, err} ->
                Logger.error(
                  "Error sending #{length(available_acks)} acks from consumer #{state.consumer_id} for topic #{
                    state.topic_name
                  }, #{inspect(err)}"
                )

                :telemetry.execute(
                  [:pulsar_ex, :consumer, :ack, :error],
                  %{count: 1, acks: length(available_acks)},
                  state.metadata
                )

                {:stop, {:error, err}, state}
            end
        end
      end

      @impl true
      def handle_info(:nacks, %{state: :connecting} = state) do
        Process.send_after(self(), :nacks, state.redelivery_interval)
        {:noreply, state}
      end

      @impl true
      def handle_info(:nacks, state) do
        {available_nacks, acks} =
          Enum.split_with(state.acks, fn
            {false, ts} -> Timex.after?(Timex.now(), ts)
            _ -> false
          end)

        cond do
          length(available_nacks) == 0 ->
            Process.send_after(self(), :nacks, state.redelivery_interval)
            {:noreply, state}

          true ->
            start = System.monotonic_time(:millisecond)

            available_nacks =
              Enum.map(available_nacks, fn {{ledgerId, entryId}, _} ->
                {ledgerId, entryId}
              end)

            case Connection.redeliver(state.connection, state.consumer_id, available_nacks) do
              :ok ->
                Logger.debug(
                  "Sent #{length(available_nacks)} nacks from consumer #{state.consumer_id} for topic #{
                    state.topic_name
                  }"
                )

                :telemetry.execute(
                  [:pulsar_ex, :consumer, :nacks, :success],
                  %{
                    count: 1,
                    nacks: length(available_nacks),
                    duration: System.monotonic_time(:millisecond) - start
                  },
                  state.metadata
                )

                Process.send_after(self(), :nacks, state.redelivery_interval)
                {:noreply, %{state | acks: Enum.into(acks, %{})}}

              {:error, err} ->
                Logger.error(
                  "Error sending #{length(available_nacks)} nacks from consumer #{
                    state.consumer_id
                  } for topc #{state.topic_name}, #{inspect(err)}"
                )

                :telemetry.execute(
                  [:pulsar_ex, :consumer, :nacks, :error],
                  %{count: 1, nacks: length(available_nacks)},
                  state.metadata
                )

                {:stop, {:error, err}, state}
            end
        end
      end

      @impl true
      def handle_info(:poll, %{state: :connecting} = state) do
        Process.send_after(self(), :poll, state.poll_interval)
        {:noreply, state}
      end

      @impl true
      def handle_info(:poll, state) do
        # In the event of shutting down, we will stop processing any the messages in queue/batch, thus generating no more acks/nacks.
        # Acks will continue being sent to broker as well as flow permits and nacks.
        # However, no more messages will be processed anymore.
        if PulsarEx.Application.shutdown?() do
          {:noreply, state}
        else
          case :queue.out(state.queue) do
            {:empty, _} ->
              handle_empty(state)

            {{:value, batch}, queue} ->
              handle_batch(batch, %{
                state
                | queue: queue,
                  queue_size: state.queue_size - state.batch_size
              })
          end
        end
      end

      defp handle_empty(%{batch: []} = state) do
        handle_flow_permits(state)
      end

      defp handle_empty(%{batch: batch} = state) do
        handle_batch(Enum.reverse(batch), %{state | batch: [], queue_size: 0})
      end

      defp handle_batch(batch, state) do
        result =
          try do
            handle_messages(batch, state)
          rescue
            err ->
              Logger.error(Exception.format(:error, err, __STACKTRACE__))

              Logger.error(
                "Error handling batch of #{length(batch)} messages from consumer #{
                  state.consumer_id
                } for topic #{state.topic_name}, #{inspect(err)}"
              )

              Enum.map(batch, fn _ -> {:error, err} end)
          end

        state =
          Enum.zip(batch, result)
          |> Enum.reduce(state, fn
            {message, :ok}, acc ->
              track_ack(message, acc)

            {message, {:ok, _}}, acc ->
              track_ack(message, acc)

            {message, _}, acc ->
              track_nack(message, acc)
          end)

        state = %{state | permits: state.permits + length(batch)}

        handle_flow_permits(state)
      end

      defp handle_flow_permits(
             %{refill_queue_size_watermark: refill_queue_size_watermark, queue_size: queue_size} =
               state
           )
           when queue_size > refill_queue_size_watermark do
        Process.send(self(), :poll, [])

        {:noreply, state}
      end

      defp handle_flow_permits(
             %{permits: permits, flow_permits_watermark: flow_permits_watermark} = state
           )
           when permits < flow_permits_watermark do
        if state.queue_size > 0 do
          Process.send(self(), :poll, [])
        else
          Process.send_after(self(), :poll, state.poll_interval)
        end

        {:noreply, state}
      end

      defp handle_flow_permits(state) do
        start = System.monotonic_time(:millisecond)

        case Connection.flow_permits(state.connection, state.consumer_id, state.permits) do
          :ok ->
            Logger.debug(
              "Sent #{state.permits} permits from consumer #{state.consumer_id} for topic #{
                state.topic_name
              }"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flow_permits, :success],
              %{
                count: 1,
                permits: state.permits,
                duration: System.monotonic_time(:millisecond) - start
              },
              state.metadata
            )

            if state.queue_size > 0 do
              Process.send(self(), :poll, [])
            else
              Process.send_after(self(), :poll, state.poll_interval)
            end

            {:noreply, %{state | permits: 0}}

          {:error, err} ->
            Logger.error(
              "Error sending #{state.permits} permits from consumer #{state.consumer_id} for topic #{
                state.topic_name
              }, #{inspect(err)}"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flow_permits, :error],
              %{count: 1, permits: state.permits},
              state.metadata
            )

            {:stop, {:error, err}, state}
        end
      end

      @impl true
      def handle_cast({:messages, messages}, %State{} = state) do
        Logger.debug(
          "Received #{length(messages)} messages for consumer #{state.consumer_id} from topic #{
            state.topic_name
          }"
        )

        :telemetry.execute(
          [:pulsar_ex, :consumer, :received],
          %{count: length(messages)},
          state.metadata
        )

        {compacted, messages} =
          Enum.split_with(messages, &(&1.compacted_out && !state.read_compacted))

        Logger.debug(
          "Received #{length(compacted)} compacted messages for consumer #{state.consumer_id} from topic #{
            state.topic_name
          }"
        )

        :telemetry.execute(
          [:pulsar_ex, :consumer, :received, :compacted],
          %{count: length(compacted)},
          state.metadata
        )

        state =
          Enum.reduce(compacted, state, fn message, acc ->
            track_ack(message, acc)
          end)

        {batch_acked, messages} = Enum.split_with(messages, &match?(%{batch_acked: true}, &1))

        Logger.debug(
          "Received #{length(batch_acked)} acked batch messages for consumer #{state.consumer_id} from topic #{
            state.topic_name
          }"
        )

        :telemetry.execute(
          [:pulsar_ex, :consumer, :received, :batch_acked],
          %{count: length(batch_acked)},
          state.metadata
        )

        state =
          Enum.reduce(batch_acked, state, fn message, acc ->
            track_ack(message, acc)
          end)

        {dead_letters, messages} =
          Enum.split_with(messages, fn message ->
            message.redelivery_count > state.max_redelivery_attempts
          end)

        Logger.debug(
          "Received #{length(dead_letters)} dead letter messages for consumer #{state.consumer_id} from topic #{
            state.topic_name
          }"
        )

        :telemetry.execute(
          [:pulsar_ex, :consumer, :received, :dead_letters],
          %{count: length(dead_letters)},
          state.metadata
        )

        if state.dead_letter_topic != nil do
          Enum.each(dead_letters, fn message ->
            message_opts =
              Map.take(message, [:properties, :partition_key, :ordering_key, :event_time])

            PulsarEx.produce(
              state.dead_letter_topic,
              message.payload,
              message_opts,
              state.dead_letter_producer_opts
            )
          end)
        end

        state =
          Enum.reduce(dead_letters, state, fn message, acc ->
            track_ack(message, acc)
          end)

        {queue, batch} =
          Enum.reduce(messages, {state.queue, state.batch}, fn message, {queue, batch} ->
            if length(batch) + 1 == state.batch_size do
              {:queue.in(Enum.reverse([message | batch]), queue), []}
            else
              {queue, [message | batch]}
            end
          end)

        {:noreply,
         %State{
           state
           | queue: queue,
             batch: batch,
             queue_size: state.queue_size + length(messages),
             permits:
               state.permits + length(compacted) + length(batch_acked) + length(dead_letters)
         }}
      end

      @impl true
      def handle_cast(:close, state) do
        Logger.warn("Received close command from connection for topic #{state.topic_name}")

        :telemetry.execute(
          [:pulsar_ex, :consumer, :close],
          %{count: 1},
          state.metadata
        )

        Process.send_after(self(), :connect, @connection_interval)
        {:noreply, %{state | state: :connecting}}
      end

      @impl true
      def handle_messages(messages, _state) do
        Enum.map(messages, &Logger.info/1)
      end

      defoverridable handle_messages: 2

      @impl true
      def terminate(reason, state) do
        Connection.stop_consumer(state.connection, state.consumer_id)

        if Enum.count(state.acks) > 0 do
          Logger.error(
            "Stopping consumer while #{Enum.count(state.acks)} acks are still left in consumer #{
              state.consumer_id
            } for topic #{state.topic_name}"
          )
        end

        case reason do
          :shutdown ->
            Logger.debug(
              "Stopping consumer #{state.consumer_id} for topic #{state.topic_name}, #{
                inspect(reason)
              }"
            )

            state

          :normal ->
            Logger.debug(
              "Stopping consumer #{state.consumer_id} for topic #{state.topic_name}, #{
                inspect(reason)
              }"
            )

            state

          {:shutdown, _} ->
            Logger.debug(
              "Stopping consumer #{state.consumer_id} for topic #{state.topic_name}, #{
                inspect(reason)
              }"
            )

            state

          _ ->
            Logger.error(
              "Stopping consumer #{state.consumer_id} for topic #{state.topic_name}, #{
                inspect(reason)
              }"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :exit],
              %{count: 1},
              state.metadata
            )

            state
        end
      end

      defp track_ack(%{message_id: message_id, batch_size: batch_size} = message, state)
           when batch_size > 1 do
        key = {message_id.ledgerId, message_id.entryId}

        value =
          case Map.get(state.acks, key) do
            nil ->
              message.ack_set

            true ->
              true

            {false, ts} ->
              {false, ts}

            ack_set ->
              bitset =
                Bitset.from_words(ack_set, batch_size)
                |> Bitset.and_set(Bitset.from_words(message.ack_set, batch_size))

              if bitset == Bitset.new(batch_size) do
                true
              else
                Bitset.to_words(bitset)
              end
          end

        acks = Map.put(state.acks, key, value)
        %{state | acks: acks}
      end

      defp track_ack(%{message_id: message_id}, state) do
        key = {message_id.ledgerId, message_id.entryId}
        acks = Map.put(state.acks, key, true)
        %{state | acks: acks}
      end

      defp track_nack(%{message_id: message_id, redelivery_count: redelivery_count}, state) do
        resend_ts =
          case state.redelivery_policy do
            :exp ->
              Timex.add(
                Timex.now(),
                Timex.Duration.from_milliseconds(
                  trunc(:math.pow(2, redelivery_count)) * state.redelivery_interval
                )
              )

            _ ->
              Timex.add(
                Timex.now(),
                Timex.Duration.from_milliseconds(state.redelivery_interval)
              )
          end

        key = {message_id.ledgerId, message_id.entryId}
        acks = Map.put(state.acks, key, {false, resend_ts})
        %{state | acks: acks}
      end
    end
  end
end
