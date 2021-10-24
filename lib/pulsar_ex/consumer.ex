defmodule PulsarEx.Consumer do
  defmodule State do
    @enforce_keys [
      :state,
      :topic,
      :topic_name,
      :subscription,
      :brokers,
      :admin_port,
      :subscription_type,
      :receiving_queue_size,
      :refill_queue_size_watermark,
      :flow_permits_watermark,
      :batch_size,
      :max_redelivery_attempts,
      :redelivery_policy,
      :dead_letter_topic,
      :poll_interval,
      :ack_interval,
      :redelivery_interval,
      :dead_letter_interval,
      :dead_letter_attempts,
      :max_dead_letter_attempts,
      :dead_letter_producer_opts,
      :termination_timeout,
      :permits,
      :queue,
      :queue_size,
      :batch,
      :acks,
      :pending_acks,
      :nacks,
      :dead_letters,
      :passive_mode,
      :consumer_opts,
      :metadata
    ]
    defstruct [
      :state,
      :topic,
      :topic_name,
      :subscription,
      :brokers,
      :admin_port,
      :broker,
      :subscription_type,
      :priority_level,
      :read_compacted,
      :initial_position,
      :consumer_id,
      :consumer_name,
      :properties,
      :connection,
      :receiving_queue_size,
      :refill_queue_size_watermark,
      :flow_permits_watermark,
      :batch_size,
      :max_redelivery_attempts,
      :redelivery_policy,
      :dead_letter_topic,
      :poll_interval,
      :ack_interval,
      :redelivery_interval,
      :dead_letter_interval,
      :dead_letter_attempts,
      :max_dead_letter_attempts,
      :dead_letter_producer_opts,
      :termination_timeout,
      :permits,
      :queue,
      :queue_size,
      :batch,
      :acks,
      :pending_acks,
      :nacks,
      :dead_letters,
      :passive_mode,
      :consumer_opts,
      :metadata
    ]
  end

  def poll(consumer) do
    GenServer.call(consumer, :poll)
  end

  def ack(consumer, message) when not is_list(message), do: ack(consumer, [message])

  def ack(consumer, messages) do
    GenServer.cast(consumer, {:ack, messages})
  end

  def nack(consumer, message) when not is_list(message), do: nack(consumer, [message])

  def nack(consumer, messages) do
    GenServer.cast(consumer, {:nack, messages})
  end

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      alias PulsarEx.{
        Topic,
        Admin,
        ConnectionManager,
        Connection,
        ConsumerCallback,
        ConsumerRegistry
      }

      alias PulsarEx.Consumer.State

      require Logger

      use GenServer
      @behaviour ConsumerCallback

      @passive_mode Keyword.get(opts, :passive_mode, false)
      @subscription_type Keyword.get(opts, :subscription_type, :shared)
      @receiving_queue_size Keyword.get(opts, :receiving_queue_size, 100)
      @flow_control_watermark Keyword.get(opts, :flow_control_watermark, 0.5)
      @batch_size Keyword.get(opts, :batch_size, 1)
      @max_redelivery_attempts Keyword.get(opts, :max_redelivery_attempts, 3)
      @redelivery_policy Keyword.get(opts, :redelivery_policy, :exp)
      @dead_letter_topic Keyword.get(opts, :dead_letter_topic, nil)
      @poll_interval Keyword.get(opts, :poll_interval, 50)
      @ack_interval Keyword.get(opts, :ack_interval, 5_000)
      @redelivery_interval Keyword.get(opts, :redelivery_interval, 1_000)
      @dead_letter_interval Keyword.get(opts, :dead_letter_interval, 5_000)
      @max_dead_letter_attempts Keyword.get(opts, :max_dead_letter_attempts, 5)
      @dead_letter_producer_opts Keyword.get(opts, :dead_letter_producer_opts,
                                   batch_enabled: true,
                                   batch_size: 100,
                                   flush_interval: 1_000
                                 )
      @termination_timeout Keyword.get(opts, :termination_timeout, 1000)

      def start_link({topic, subscription, consumer_opts}) do
        GenServer.start_link(__MODULE__, {topic, subscription, consumer_opts})
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

        subscription_type = Keyword.get(consumer_opts, :subscription_type, @subscription_type)

        passive_mode = Keyword.get(consumer_opts, :passive_mode, @passive_mode)

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

        dead_letter_interval =
          max(Keyword.get(consumer_opts, :dead_letter_interval, @dead_letter_interval), 5_000)

        max_dead_letter_attempts =
          min(
            Keyword.get(consumer_opts, :max_dead_letter_attempts, @max_dead_letter_attempts),
            10
          )

        dead_letter_producer_opts =
          Keyword.get(consumer_opts, :dead_letter_producer_opts, @dead_letter_producer_opts)

        termination_timeout =
          min(Keyword.get(consumer_opts, :termination_timeout, @termination_timeout), 5_000)

        state = %State{
          state: :init,
          topic: topic,
          topic_name: topic_name,
          subscription: subscription,
          brokers: brokers,
          admin_port: admin_port,
          subscription_type: subscription_type,
          receiving_queue_size: receiving_queue_size,
          refill_queue_size_watermark: refill_queue_size_watermark,
          flow_permits_watermark: flow_permits_watermark,
          batch_size: batch_size,
          max_redelivery_attempts: max_redelivery_attempts,
          redelivery_policy: redelivery_policy,
          dead_letter_topic: dead_letter_topic,
          poll_interval: poll_interval,
          ack_interval: ack_interval,
          redelivery_interval: redelivery_interval,
          dead_letter_interval: dead_letter_interval,
          dead_letter_attempts: 0,
          max_dead_letter_attempts: max_dead_letter_attempts,
          dead_letter_producer_opts: dead_letter_producer_opts,
          termination_timeout: termination_timeout,
          permits: receiving_queue_size,
          queue: :queue.new(),
          queue_size: 0,
          batch: [],
          acks: [],
          pending_acks: %{},
          nacks: [],
          dead_letters: [],
          passive_mode: passive_mode,
          consumer_opts: consumer_opts,
          metadata: metadata
        }

        Process.send(self(), :init, [])

        {:ok, state}
      end

      defp subscribe(pool, topic_name, subscription, subscription_type, consumer_opts) do
        connection = :poolboy.checkout(pool)
        :poolboy.checkin(pool, connection)

        Connection.subscribe(
          connection,
          topic_name,
          subscription,
          subscription_type,
          consumer_opts
        )
      end

      @impl true
      def handle_info(:init, %{state: :init} = state) do
        with {:ok, broker} <- Admin.lookup_topic(state.brokers, state.admin_port, state.topic),
             {:ok, pool} <- ConnectionManager.get_connection(broker),
             {:ok, reply} <-
               subscribe(
                 pool,
                 state.topic_name,
                 state.subscription,
                 state.subscription_type,
                 state.consumer_opts
               ) do
          %{
            priority_level: priority_level,
            read_compacted: read_compacted,
            initial_position: initial_position,
            consumer_id: consumer_id,
            consumer_name: consumer_name,
            properties: properties,
            connection: connection
          } = reply

          Process.monitor(connection)

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
              consumer_id: consumer_id,
              consumer_name: consumer_name,
              properties: properties,
              connection: connection,
              metadata: metadata
          }

          unless state.passive_mode do
            Process.send(self(), :poll, [])
          end

          Process.send_after(self(), :acks, state.ack_interval)
          Process.send_after(self(), :nacks, state.redelivery_interval)
          Process.send_after(self(), :dead_letters, state.dead_letter_interval)

          Logger.debug(
            "Started consumer for topic #{state.topic_name} with subscription #{
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
            :telemetry.execute(
              [:pulsar_ex, :consumer, :connect, :error],
              %{count: 1},
              state.metadata
            )

            {:stop, err, state}
        end
      end

      @impl true
      def handle_info({:DOWN, _, _, _, _}, state) do
        Logger.error("Connection down for consumer with topic #{state.topic_name}")

        {:stop, {:error, :connection_down}, state}
      end

      @impl true
      def handle_info(:acks, %{acks: []} = state) do
        Process.send_after(self(), :acks, state.ack_interval)
        {:noreply, state}
      end

      @impl true
      def handle_info(:acks, %{acks: acks} = state) do
        case Connection.ack(
               state.connection,
               state.consumer_id,
               :individual,
               acks
             ) do
          {:ok, request_id} ->
            Logger.debug(
              "Sent #{length(acks)} acks from consumer #{state.consumer_id} for topic #{
                state.topic_name
              }"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flush_acks, :success],
              %{count: 1, acks: length(acks)},
              state.metadata
            )

            Process.send_after(self(), :acks, state.ack_interval)

            {:noreply,
             %{state | acks: [], pending_acks: Map.put(state.pending_acks, request_id, acks)}}

          {:error, err} ->
            Logger.error(
              "Error sending #{length(acks)} acks from consumer #{state.consumer_id} for topic #{
                state.topic_name
              }, #{inspect(err)}"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flush_acks, :error],
              %{count: 1, acks: length(acks)},
              state.metadata
            )

            {:stop, {:error, err}, state}
        end
      end

      @impl true
      def handle_info(:nacks, %{nacks: []} = state) do
        Process.send_after(self(), :nacks, state.redelivery_interval)
        {:noreply, state}
      end

      @impl true
      def handle_info(:nacks, %{nacks: nacks} = state) do
        {resend_messages, nacks} =
          Enum.split_with(nacks, fn {_, resend_ts} -> Timex.after?(Timex.now(), resend_ts) end)

        message_ids = Enum.map(resend_messages, fn {message_id, _} -> message_id end)

        if length(message_ids) > 0 do
          case Connection.redeliver(state.connection, state.consumer_id, message_ids) do
            :ok ->
              Logger.debug(
                "Sent #{length(message_ids)} nacks from consumer #{state.consumer_id} for topic #{
                  state.topic_name
                }"
              )

              :telemetry.execute(
                [:pulsar_ex, :consumer, :flush_nacks, :success],
                %{count: 1, nacks: length(message_ids)},
                state.metadata
              )

              Process.send_after(self(), :nacks, state.redelivery_interval)
              {:noreply, %{state | nacks: nacks}}

            {:error, err} ->
              Logger.error(
                "Error sending #{length(message_ids)} nacks from consumer #{state.consumer_id} for topc #{
                  state.topic_name
                }, #{inspect(err)}"
              )

              :telemetry.execute(
                [:pulsar_ex, :consumer, :flush_nacks, :error],
                %{count: 1, nacks: length(message_ids)},
                state.metadata
              )

              {:stop, {:error, err}, state}
          end
        else
          Process.send_after(self(), :nacks, state.redelivery_interval)
          {:noreply, state}
        end
      end

      @impl true
      def handle_info(:dead_letters, %{dead_letters: []} = state) do
        Process.send_after(self(), :dead_letters, state.dead_letter_interval)

        {:noreply, state}
      end

      @impl true
      def handle_info(
            :dead_letters,
            %{dead_letter_topic: nil, dead_letters: dead_letters} = state
          ) do
        Logger.warn(
          "Purged #{length(dead_letters)} dead letters from consumer #{state.consumer_id} for topic #{
            state.topic_name
          }"
        )

        Process.send_after(self(), :dead_letters, state.dead_letter_interval)

        {:noreply, %{state | dead_letters: []}}
      end

      @impl true
      def handle_info(:dead_letters, %{dead_letters: dead_letters} = state) do
        {err, remain} =
          dead_letters
          |> Enum.reduce({nil, []}, fn message, {err, remain} ->
            message_opts =
              Map.take(message, [:properties, :partition_key, :ordering_key, :event_time])

            PulsarEx.sync_produce(
              state.dead_letter_topic,
              message.payload,
              message_opts,
              state.dead_letter_producer_opts
            )
            |> case do
              :ok -> {err, remain}
              {:error, error} -> {err || error, [message | remain]}
            end
          end)

        case err do
          nil ->
            Logger.warn(
              "Sent #{length(dead_letters)} dead letters to topic #{state.dead_letter_topic}, from consumer #{
                state.consumer_id
              } for topic #{state.topic_name}"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flush_dead_letters, :success],
              %{count: 1, dead_letters: length(dead_letters)},
              state.metadata
            )

            Process.send_after(self(), :dead_letters, state.dead_letter_interval)

            {:noreply, %{state | dead_letters: [], dead_letter_attempts: 0}}

          _ ->
            Logger.error(
              "Error senting #{length(remain)} dead letters to topic #{state.dead_letter_topic}, from consumer #{
                state.consumer_id
              } for topic #{state.topic_name} with #{state.dead_letter_attempts} attempts, #{
                inspect(err)
              }"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flush_dead_letters, :error],
              %{count: 1, dead_letters: length(remain)},
              state.metadata
            )

            state = %{
              state
              | dead_letters: remain,
                dead_letter_attempts: state.dead_letter_attempts + 1
            }

            if state.dead_letter_attempts < state.max_dead_letter_attempts do
              Process.send_after(self(), :dead_letters, state.dead_letter_interval)
              {:noreply, state}
            else
              {:stop, {:error, err}, state}
            end
        end
      end

      @impl true
      def handle_info(:poll, state) do
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
          catch
            :exit, reason ->
              Logger.error(Exception.format(:error, reason, __STACKTRACE__))

              Logger.error(
                "Error handling batch of #{length(batch)} messages from consumer #{
                  state.consumer_id
                } for topic #{state.topic_name}, #{inspect(reason)}"
              )

              Enum.map(batch, fn _ -> {:error, reason} end)
          end

        {acks, nacks} =
          Enum.zip(batch, result)
          |> Enum.reduce({[], []}, fn
            {message, :ok}, {acks, nacks} ->
              {[message.message_id | acks], nacks}

            {message, {:ok, _}}, {acks, nacks} ->
              {[message.message_id | acks], nacks}

            {message, _}, {acks, nacks} ->
              resend_ts =
                case state.redelivery_policy do
                  :exp ->
                    Timex.add(
                      Timex.now(),
                      Timex.Duration.from_milliseconds(
                        trunc(:math.pow(2, message.redelivery_count)) * state.redelivery_interval
                      )
                    )

                  _ ->
                    Timex.add(
                      Timex.now(),
                      Timex.Duration.from_milliseconds(state.redelivery_interval)
                    )
                end

              nack = {message.message_id, resend_ts}
              {acks, [nack | nacks]}
          end)

        :telemetry.execute(
          [:pulsar_ex, :consumer, :acks],
          %{count: length(acks)},
          state.metadata
        )

        :telemetry.execute(
          [:pulsar_ex, :consumer, :nacks],
          %{count: length(nacks)},
          state.metadata
        )

        state = %{
          state
          | acks: state.acks ++ acks,
            nacks: state.nacks ++ nacks,
            permits: state.permits + length(acks)
        }

        handle_flow_permits(state)
      end

      defp handle_flow_permits(
             %{refill_queue_size_watermark: refill_queue_size_watermark, queue_size: queue_size} =
               state
           )
           when queue_size >= refill_queue_size_watermark do
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
        permits = min(state.receiving_queue_size - state.queue_size, state.permits)

        case Connection.flow_permits(state.connection, state.consumer_id, permits) do
          :ok ->
            Logger.debug(
              "Sent #{permits} permits from consumer #{state.consumer_id} for topic #{
                state.topic_name
              }"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flow_permits, :success],
              %{count: 1, permits: permits},
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
              "Error sending #{permits} permits from consumer #{state.consumer_id} for topic #{
                state.topic_name
              }, #{inspect(err)}"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flow_permits, :error],
              %{count: 1, permits: permits},
              state.metadata
            )

            {:stop, {:error, err}, state}
        end
      end

      @impl true
      def handle_call(:poll, _from, state) do
        {batch, state} =
          case {:queue.out(state.queue), state.batch} do
            {{:empty, _}, []} ->
              {[], state}

            {{:empty, _}, batch} ->
              {Enum.reverse(batch),
               %{state | batch: [], queue_size: state.queue_size - length(batch)}}

            {{{:value, batch}, queue}, _} ->
              {batch, %{state | queue: queue, queue_size: state.queue_size - state.batch_size}}
          end

        case flow_permits(state) do
          {:ok, state} ->
            {:reply, Enum.map(batch, &%{&1 | consumer: self()}), state}

          {:error, err, state} ->
            {:stop, {:error, err}, {:error, err}, state}
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

        acks = Enum.map(compacted, & &1.message_id)

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

        acks = acks ++ Enum.map(dead_letters, & &1.message_id)

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
             acks: state.acks ++ acks,
             dead_letters: state.dead_letters ++ dead_letters,
             permits:
               state.permits + length(compacted) + length(batch_acked) + length(dead_letters)
         }}
      end

      @impl true
      def handle_cast(:close, state) do
        Logger.warn("Received close command from connection for topic #{state.topic_name}")

        {:stop, {:shutdown, :close}, state}
      end

      @impl true
      def handle_cast({:ack, messages}, state) do
        acks = messages |> Enum.map(& &1.message_id)

        :telemetry.execute(
          [:pulsar_ex, :consumer, :acks],
          %{count: length(messages)},
          state.metadata
        )

        {:noreply, %{state | acks: state.acks ++ acks, permits: state.permits + length(acks)}}
      end

      @impl true
      def handle_cast({:nack, messages}, state) do
        :telemetry.execute(
          [:pulsar_ex, :consumer, :nacks],
          %{count: length(messages)},
          state.metadata
        )

        nacks =
          messages
          |> Enum.map(fn message ->
            resend_ts =
              case state.redelivery_policy do
                :exp ->
                  Timex.add(
                    Timex.now(),
                    Timex.Duration.from_milliseconds(
                      trunc(:math.pow(2, message.redelivery_count)) * state.redelivery_interval
                    )
                  )

                _ ->
                  Timex.add(
                    Timex.now(),
                    Timex.Duration.from_milliseconds(state.redelivery_interval)
                  )
              end

            {message.message_id, resend_ts}
          end)

        {:noreply, %{state | nacks: state.nacks ++ nacks}}
      end

      @impl true
      def handle_cast({:ack_response, {:ok, request_id}}, state) do
        {acks, pending_acks} = Map.pop(state.pending_acks, request_id, [])
        Logger.debug("Received successful ack response for #{length(acks)} acks")

        :telemetry.execute(
          [:pulsar_ex, :consumer, :ack_response, :success],
          %{count: 1, acks: length(acks)},
          state.metadata
        )

        {:noreply, %{state | pending_acks: pending_acks}}
      end

      @impl true
      def handle_cast({:ack_response, {:error, err, request_id}}, state) do
        {acks, pending_acks} = Map.pop(state.pending_acks, request_id, [])
        Logger.error("Received failed ack response for #{length(acks)} acks, #{inspect(err)}")

        :telemetry.execute(
          [:pulsar_ex, :consumer, :ack_response, :error],
          %{count: 1, acks: length(acks)},
          state.metadata
        )

        {:noreply, %{state | pending_acks: pending_acks, acks: state.acks ++ acks}}
      end

      defp flow_permits(
             %{refill_queue_size_watermark: refill_queue_size_watermark, queue_size: queue_size} =
               state
           )
           when queue_size >= refill_queue_size_watermark do
        {:ok, state}
      end

      defp flow_permits(
             %{permits: permits, flow_permits_watermark: flow_permits_watermark} = state
           )
           when permits < flow_permits_watermark do
        {:ok, state}
      end

      defp flow_permits(state) do
        permits = min(state.receiving_queue_size - state.queue_size, state.permits)

        case Connection.flow_permits(state.connection, state.consumer_id, permits) do
          :ok ->
            Logger.debug(
              "Sent #{permits} permits from consumer #{state.consumer_id} for topic #{
                state.topic_name
              }"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flow_permits, :success],
              %{count: 1, permits: permits},
              state.metadata
            )

            {:ok, %{state | permits: 0}}

          {:error, err} ->
            Logger.error(
              "Error sending #{permits} permits from consumer #{state.consumer_id} for topic #{
                state.topic_name
              }, #{inspect(err)}"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flow_permits, :error],
              %{count: 1, permits: permits},
              state.metadata
            )

            {:error, err, state}
        end
      end

      @impl true
      def handle_messages(messages, _state) do
        Enum.map(messages, &Logger.info/1)
      end

      defoverridable handle_messages: 2

      @impl true
      def terminate(reason, state) do
        if length(state.acks) > 0 do
          Logger.error(
            "Stopping consumer while #{length(state.acks)} acks are still left in consumer #{
              state.consumer_id
            } for topic #{state.topic_name}"
          )
        end

        if length(state.dead_letters) > 0 do
          Logger.error(
            "Stopping consumer while #{length(state.dead_letters)} dead letters are still left in consumer #{
              state.consumer_id
            } for topic #{state.topic_name}"
          )

          # best effort
          if state.dead_letter_topic != nil do
            Enum.each(state.dead_letters, fn message ->
              message_opts =
                Map.take(message, [:properties, :partition_key, :ordering_key, :event_time])

              PulsarEx.async_produce(
                state.dead_letter_topic,
                message.payload,
                message_opts,
                state.dead_letter_producer_opts
              )
            end)

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flush_dead_letters, :best_effort],
              %{count: 1, dead_letters: length(state.dead_letters)},
              state.metadata
            )
          end
        end

        if length(state.nacks) > 0 do
          Logger.warn(
            "Stopping consumer while #{length(state.nacks)} nacks are still left in consumer #{
              state.consumer_id
            } for topic #{state.topic_name}"
          )
        end

        if state.queue_size > 0 do
          Logger.warn(
            "Stopping consumer while #{state.queue_size} messages are still left in consumer #{
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

          {:shutdown, :close} ->
            Logger.debug(
              "Stopping consumer #{state.consumer_id} for topic #{state.topic_name}, #{
                inspect(reason)
              }"
            )

            :telemetry.execute(
              [:pulsar_ex, :consumer, :close],
              %{count: 1},
              state.metadata
            )

            # avoid immediate recreate on broker
            Process.sleep(state.termination_timeout)
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

            # avoid immediate recreate on broker
            Process.sleep(state.termination_timeout)
            state
        end
      end
    end
  end
end
