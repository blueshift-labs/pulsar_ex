defmodule PulsarEx.Consumer do
  @enforce_keys [
    :state,
    :cluster,
    :topic,
    :consumer_id,
    :subscription,
    :subscription_type,
    :consumer_opts,
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
    :ack_timeout,
    :max_redirects,
    :max_attempts,
    :redirects,
    :attempts,
    :backoff,
    :permits,
    :queue,
    :queue_size,
    :batch,
    :acks,
    :pending_acks,
    :messages_received,
    :messages_acked,
    :messages_nacked,
    :messages_dead_lettered,
    :flow_permits_sent,
    :authoritative
  ]

  defstruct [
    :state,
    :cluster,
    :topic,
    :consumer_id,
    :subscription,
    :subscription_type,
    :consumer_opts,
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
    :ack_timeout,
    :max_redirects,
    :max_attempts,
    :redirects,
    :attempts,
    :backoff,
    :permits,
    :queue,
    :queue_size,
    :batch,
    :acks,
    :pending_acks,
    :messages_received,
    :messages_acked,
    :messages_nacked,
    :messages_dead_lettered,
    :flow_permits_sent,
    :authoritative,
    :broker_url,
    :connection,
    :connection_monitor,
    :consumer_name,
    :initial_position,
    :priority_level,
    :read_compacted,
    :properties,
    :error
  ]

  def close(consumer) do
    GenServer.cast(consumer, :close)
  end

  def messages(consumer, messages) do
    GenServer.cast(consumer, {:messages, messages})
  end

  def ack_response(consumer, response) do
    GenServer.cast(consumer, {:ack_response, response})
  end

  def messages_received(consumer, timeout) do
    GenServer.call(consumer, :messages_received, timeout)
  end

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      use GenServer

      alias PulsarEx.Application, as: App

      alias PulsarEx.{
        Cluster,
        Broker,
        Topic,
        AckSet,
        Backoff,
        ConnectionManager,
        Connection,
        Consumer,
        ConsumerMessage,
        ConsumerCallback,
        ConsumerRegistry,
        ConsumerIDRegistry
      }

      require Logger

      @behaviour ConsumerCallback

      @backoff_type :rand_exp
      @backoff_min 1_000
      @backoff_max 10_000
      @backoff Backoff.new(
                 backoff_type: @backoff_type,
                 backoff_min: @backoff_min,
                 backoff_max: @backoff_max
               )

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
      @dead_letter_producer_opts Keyword.get(opts, :dead_letter_producer_opts, [])
      @ack_timeout Keyword.get(opts, :ack_timeout, 5000)
      @lookup_timeout Keyword.get(opts, :lookup_timeout, 30_000)
      @connect_timeout Keyword.get(opts, :connect_timeout, 30_000)
      @max_redirects Keyword.get(opts, :max_redirects, 20)
      @max_attempts Keyword.get(opts, :max_attempts, 10)

      def start_link(
            {%Cluster{cluster_name: cluster_name} = cluster, %Topic{} = topic, subscription,
             consumer_opts}
          ) do
        consumer_id = App.get_consumer_id(cluster_name)

        GenServer.start_link(
          __MODULE__,
          {cluster, topic, consumer_id, subscription, consumer_opts},
          name:
            {:via, Registry,
             {ConsumerIDRegistry, {cluster_name, consumer_id},
              {cluster, topic, subscription, consumer_opts}}}
        )
      end

      def start_nolink(
            {%Cluster{cluster_name: cluster_name} = cluster, %Topic{} = topic, subscription,
             consumer_opts}
          ) do
        consumer_id = App.get_consumer_id(cluster_name)

        GenServer.start(
          __MODULE__,
          {cluster, topic, consumer_id, subscription, consumer_opts},
          name:
            {:via, Registry,
             {ConsumerIDRegistry, {cluster_name, consumer_id},
              {cluster, topic, subscription, consumer_opts}}}
        )
      end

      @impl true
      def init(
            {%Cluster{cluster_name: cluster_name, cluster_opts: cluster_opts} = cluster,
             %Topic{} = topic, consumer_id, subscription, consumer_opts}
          ) do
        Logger.metadata(
          cluster: "#{cluster}",
          broker: nil,
          topic: "#{topic}",
          consumer_id: consumer_id,
          subscription: subscription
        )

        Process.flag(:trap_exit, true)

        consumer_opts =
          cluster_opts
          |> Keyword.get(:consumer_opts, [])
          |> Keyword.merge(consumer_opts, fn
            :properties, v1, v2 -> Keyword.merge(v1, v2)
            _, _, v -> v
          end)

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

        redelivery_policy = Keyword.get(consumer_opts, :redelivery_policy, @redelivery_policy)

        max_redelivery_attempts =
          max(Keyword.get(consumer_opts, :max_redelivery_attempts, @max_redelivery_attempts), 1)

        dead_letter_topic =
          case Keyword.get(consumer_opts, :dead_letter_topic, @dead_letter_topic) do
            :self -> to_string(topic)
            other -> other
          end

        poll_interval = max(Keyword.get(consumer_opts, :poll_interval, @poll_interval), 10)

        ack_interval = max(Keyword.get(consumer_opts, :ack_interval, @ack_interval), 1_000)

        redelivery_interval =
          max(Keyword.get(consumer_opts, :redelivery_interval, @redelivery_interval), 1_000)

        dead_letter_producer_opts =
          Keyword.get(consumer_opts, :dead_letter_producer_opts, @dead_letter_producer_opts)

        ack_timeout = Keyword.get(consumer_opts, :ack_timeout, @ack_timeout)

        max_redirects = Keyword.get(consumer_opts, :max_redirects, @max_redirects)
        max_attempts = Keyword.get(consumer_opts, :max_attempts, @max_attempts)

        state = %Consumer{
          state: :LOOKUP,
          cluster: cluster,
          topic: topic,
          consumer_id: consumer_id,
          subscription: subscription,
          consumer_opts: consumer_opts,
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
          ack_timeout: ack_timeout,
          max_redirects: max_redirects,
          max_attempts: max_attempts,
          permits: receiving_queue_size,
          queue: :queue.new(),
          queue_size: 0,
          batch: [],
          acks: %{},
          messages_received: 0,
          messages_acked: 0,
          messages_nacked: 0,
          pending_acks: %{},
          messages_dead_lettered: 0,
          flow_permits_sent: 0,
          backoff: @backoff,
          attempts: 0,
          error: nil,
          authoritative: false,
          broker_url: nil,
          redirects: 0
        }

        Logger.debug("Starting consumer")

        Process.send(self(), :connect, [])
        Process.send_after(self(), :poll, poll_interval)
        Process.send_after(self(), :acks, ack_interval)
        Process.send_after(self(), :nacks, redelivery_interval)

        {:ok, state}
      end

      # ===================  handle_info  ===================
      @impl true
      def handle_info(
            :connect,
            %{max_attempts: max_attempts, attempts: attempts, error: err} = state
          )
          when attempts >= max_attempts do
        Logger.error("Exhausted max attempts, #{inspect(err)}")

        :telemetry.execute(
          [:pulsar_ex, :consumer, :max_attempts],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, err}, state}
      end

      def handle_info(:connect, %{max_redirects: max_redirects, redirects: redirects} = state)
          when redirects >= max_redirects do
        Logger.error("Exhausted max redirects")

        :telemetry.execute(
          [:pulsar_ex, :consumer, :max_redirects],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, :too_many_redirects}, state}
      end

      def handle_info(
            :connect,
            %{state: :LOOKUP, cluster: %Cluster{brokers: brokers, port: port}, broker_url: nil} =
              state
          ) do
        broker_url = %Broker{host: Enum.random(brokers), port: port} |> to_string()
        handle_info(:connect, %{state | broker_url: broker_url})
      end

      def handle_info(
            :connect,
            %{
              state: :LOOKUP,
              cluster: %Cluster{cluster_name: cluster_name} = cluster,
              topic: topic,
              consumer_id: consumer_id,
              subscription: subscription,
              authoritative: authoritative,
              broker_url: broker_url,
              redirects: redirects,
              attempts: attempts,
              backoff: backoff
            } = state
          ) do
        Logger.metadata(
          cluster: "#{cluster}",
          broker: "#{broker_url}",
          topic: "#{topic}",
          consumer_id: consumer_id,
          subscription: subscription
        )

        deadline = System.monotonic_time(:millisecond) + @lookup_timeout

        with {:ok, conn} <- ConnectionManager.get_connection(cluster_name, broker_url),
             {:connect, broker_url} <-
               connection_module().lookup_topic(conn, to_string(topic), authoritative, deadline) do
          Logger.debug("Successfully looked up topic")

          :telemetry.execute(
            [:pulsar_ex, :consumer, :lookup, :success],
            %{count: 1},
            state
          )

          Process.send(self(), :connect, [])

          {:noreply, %{state | state: :CONNECT, broker_url: broker_url, redirects: 0}}
        else
          {:redirect, broker_url, authoritative} ->
            Logger.debug("Redirect topic to broker [#{broker_url}]")

            :telemetry.execute(
              [:pulsar_ex, :consumer, :lookup, :redirects],
              %{count: 1},
              state
            )

            Process.send(self(), :connect, [])

            {:noreply,
             %{
               state
               | authoritative: authoritative,
                 broker_url: broker_url,
                 redirects: redirects + 1
             }}

          {:error, err} ->
            Logger.error("Error looking up topic, #{inspect(err)}")

            :telemetry.execute(
              [:pulsar_ex, :consumer, :lookup, :error],
              %{count: 1},
              state
            )

            {wait, backoff} = Backoff.backoff(backoff)
            Process.send_after(self(), :connect, wait)

            {:noreply,
             %{
               state
               | authoritative: false,
                 broker_url: nil,
                 redirects: 0,
                 attempts: attempts + 1,
                 backoff: backoff,
                 error: err
             }}
        end
      end

      def handle_info(
            :connect,
            %{
              state: :CONNECT,
              cluster: %Cluster{cluster_name: cluster_name},
              topic: topic,
              consumer_id: consumer_id,
              subscription: subscription,
              subscription_type: subscription_type,
              consumer_opts: consumer_opts,
              attempts: attempts,
              backoff: backoff,
              broker_url: broker_url
            } = state
          ) do
        deadline = System.monotonic_time(:millisecond) + @connect_timeout

        with {:ok, conn} <- ConnectionManager.get_connection(cluster_name, broker_url),
             {:ok,
              %{
                consumer_name: consumer_name,
                subscription_type: subscription_type,
                priority_level: priority_level,
                read_compacted: read_compacted,
                initial_position: initial_position,
                properties: properties
              }} <-
               connection_module().subscribe(
                 conn,
                 consumer_id,
                 to_string(topic),
                 subscription,
                 subscription_type,
                 consumer_opts,
                 deadline
               ) do
          Logger.debug("Successfully created consumer")

          :telemetry.execute(
            [:pulsar_ex, :consumer, :connect, :success],
            %{count: 1},
            state
          )

          ref = Process.monitor(conn)

          {:noreply,
           %{
             state
             | state: :READY,
               redirects: 0,
               attempts: 0,
               backoff: @backoff,
               error: nil,
               connection: conn,
               connection_monitor: ref,
               consumer_name: consumer_name,
               subscription_type: subscription_type,
               priority_level: priority_level,
               read_compacted: read_compacted,
               initial_position: initial_position,
               properties: properties
           }}
        else
          {:error, err} ->
            Logger.error("Error creating consumer, #{inspect(err)}")

            :telemetry.execute(
              [:pulsar_ex, :consumer, :connect, :error],
              %{count: 1},
              state
            )

            {wait, backoff} = Backoff.backoff(backoff)
            Process.send_after(self(), :connect, wait)

            {:noreply,
             %{
               state
               | state: :LOOKUP,
                 authoritative: false,
                 broker_url: nil,
                 connection: nil,
                 redirects: 0,
                 attempts: attempts + 1,
                 backoff: backoff,
                 error: err
             }}
        end
      end

      def handle_info(
            :poll,
            %{state: :READY, queue: queue, queue_size: queue_size, batch_size: batch_size} = state
          ) do
        # In the event of shutting down, we will stop processing any the messages in queue/batch, thus generating no more acks/nacks.
        # Acks will continue being sent to broker as well as flow permits and nacks.
        # However, no more messages will be processed anymore.
        if App.shutdown?() do
          {:noreply, state}
        else
          :telemetry.execute(
            [:pulsar_ex, :consumer, :queue],
            %{size: queue_size},
            state
          )

          case :queue.out(queue) do
            {:empty, _} ->
              handle_empty(state)

            {{:value, batch}, queue} ->
              handle_batch(batch, %{state | queue: queue, queue_size: queue_size - batch_size})
          end
        end
      end

      def handle_info(:poll, %{poll_interval: poll_interval} = state) do
        Process.send_after(self(), :poll, poll_interval)
        {:noreply, state}
      end

      def handle_info(
            :acks,
            %{
              state: :READY,
              cluster: %Cluster{cluster_name: cluster_name},
              connection: conn,
              consumer_id: consumer_id,
              pending_acks: pending_acks,
              ack_timeout: ack_timeout,
              acks: acks,
              ack_interval: ack_interval,
              messages_acked: messages_acked
            } = state
          ) do
        :telemetry.execute(
          [:pulsar_ex, :consumer, :pending_acks],
          %{size: Enum.count(pending_acks)},
          state
        )

        :telemetry.execute(
          [:pulsar_ex, :consumer, :acks],
          %{size: Enum.count(acks)},
          state
        )

        {timeout_acks, pending_acks} =
          Enum.split_with(pending_acks, fn {_, ts} ->
            System.monotonic_time(:millisecond) - ts > ack_timeout
          end)

        timeout_acks = Enum.map(timeout_acks, fn {message_id, _} -> message_id end)
        pending_acks = Enum.into(pending_acks, %{})

        if length(timeout_acks) > 0 do
          :telemetry.execute(
            [:pulsar_ex, :consumer, :ack_timeout],
            %{count: length(timeout_acks)},
            state
          )
        end

        {available_acks, acks} = Enum.split_with(acks, &match?({_, {true, _}}, &1))

        cond do
          length(available_acks) == 0 && length(timeout_acks) == 0 ->
            Process.send_after(self(), :acks, ack_interval)
            {:noreply, state}

          true ->
            start = System.monotonic_time()

            {available_acks, batch_sizes} =
              Enum.map(available_acks, fn {{ledgerId, entryId}, {true, batch_size}} ->
                {{ledgerId, entryId}, batch_size}
              end)
              |> Enum.unzip()

            total_acks = Enum.sum(batch_sizes)
            available_acks = available_acks ++ timeout_acks

            deadline = System.monotonic_time(:millisecond) + ack_timeout

            case connection_module().ack(
                   conn,
                   consumer_id,
                   :individual,
                   available_acks,
                   deadline
                 ) do
              :ok ->
                Logger.debug("Sent #{total_acks} acks")

                :telemetry.execute(
                  [:pulsar_ex, :consumer, :ack, :success],
                  %{count: 1, acks: total_acks},
                  state
                )

                Process.send_after(self(), :acks, ack_interval)

                pending_acks =
                  available_acks
                  |> Enum.reduce(pending_acks, fn message_id, acc ->
                    Map.put(acc, message_id, System.monotonic_time(:millisecond))
                  end)

                {:noreply,
                 %{
                   state
                   | acks: Enum.into(acks, %{}),
                     pending_acks: pending_acks,
                     messages_acked: messages_acked + total_acks
                 }}

              {:error, err} ->
                Logger.error("Error sending #{total_acks} acks, #{inspect(err)}")

                :telemetry.execute(
                  [:pulsar_ex, :consumer, :ack, :error],
                  %{count: 1, acks: total_acks},
                  state
                )

                {:stop, {:shutdown, err}, state}
            end
        end
      end

      def handle_info(:acks, %{ack_interval: ack_interval} = state) do
        Process.send_after(self(), :acks, ack_interval)
        {:noreply, state}
      end

      def handle_info(
            :nacks,
            %{
              state: :READY,
              acks: acks,
              connection: conn,
              consumer_id: consumer_id,
              redelivery_interval: redelivery_interval,
              messages_nacked: messages_nacked
            } = state
          ) do
        {available_nacks, acks} =
          Enum.split_with(acks, fn
            {_, {false, ts, _}} -> System.monotonic_time(:millisecond) > ts
            _ -> false
          end)

        cond do
          length(available_nacks) == 0 ->
            Process.send_after(self(), :nacks, redelivery_interval)
            {:noreply, state}

          true ->
            start = System.monotonic_time()

            {available_nacks, batch_sizes} =
              Enum.map(available_nacks, fn {{ledgerId, entryId}, {false, _, batch_size}} ->
                {{ledgerId, entryId}, batch_size}
              end)
              |> Enum.unzip()

            total_nacks = Enum.sum(batch_sizes)

            case connection_module().redeliver(conn, consumer_id, available_nacks) do
              :ok ->
                Logger.debug("Sent #{total_nacks} nacks")

                :telemetry.execute(
                  [:pulsar_ex, :consumer, :nacks, :success],
                  %{count: 1, nacks: total_nacks},
                  state
                )

                Process.send_after(self(), :nacks, redelivery_interval)

                {:noreply,
                 %{
                   state
                   | acks: Enum.into(acks, %{}),
                     messages_nacked: messages_nacked + total_nacks
                 }}

              {:error, err} ->
                Logger.error("Error sending #{total_nacks} nacks, #{inspect(err)}")

                :telemetry.execute(
                  [:pulsar_ex, :consumer, :nacks, :error],
                  %{count: 1, nacks: total_nacks},
                  state
                )

                {:stop, {:shutdown, err}, state}
            end
        end
      end

      def handle_info(:nacks, %{redelivery_interval: redelivery_interval} = state) do
        Process.send_after(self(), :nacks, redelivery_interval)
        {:noreply, state}
      end

      def handle_info({:DOWN, _, _, _, _}, state) do
        Logger.error("Connection down")

        :telemetry.execute(
          [:pulsar_ex, :consumer, :connection_down],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, :connection_down}, state}
      end

      # ===================  handle_call  ===================
      @impl true
      def handle_call(:messages_received, _from, %{messages_received: messages_received} = state) do
        {:reply, messages_received, state}
      end

      # ===================  handle_cast  ===================
      @impl true
      def handle_cast(:close, state) do
        Logger.warning("Received close command")

        :telemetry.execute(
          [:pulsar_ex, :consumer, :closed],
          %{count: 1},
          state
        )

        {:stop, {:shutdown, :closed}, state}
      end

      def handle_cast(
            {:messages, messages},
            %{
              cluster: %Cluster{cluster_name: cluster_name},
              queue: queue,
              batch: batch,
              queue_size: queue_size,
              batch_size: batch_size,
              permits: permits,
              messages_received: messages_received,
              max_redelivery_attempts: max_redelivery_attempts,
              messages_dead_lettered: messages_dead_lettered,
              dead_letter_topic: dead_letter_topic,
              dead_letter_producer_opts: dead_letter_producer_opts
            } = state
          ) do
        Logger.debug("Received #{length(messages)} messages")

        :telemetry.execute(
          [:pulsar_ex, :consumer, :received],
          %{count: length(messages)},
          state
        )

        state = %{state | messages_received: messages_received + length(messages)}

        {dead_letters, messages} =
          Enum.split_with(messages, fn %{redelivery_count: redelivery_count} ->
            redelivery_count > max_redelivery_attempts
          end)

        Logger.debug("Received #{length(dead_letters)} dead letter messages")

        :telemetry.execute(
          [:pulsar_ex, :consumer, :received, :dead_letters],
          %{count: length(dead_letters)},
          state
        )

        state = %{
          state
          | messages_dead_lettered: messages_dead_lettered + length(dead_letters)
        }

        Enum.each(dead_letters, &send_to_dead_letter(&1, state))

        state =
          Enum.reduce(dead_letters, state, fn message, acc ->
            track_ack(message, acc)
          end)

        {queue, batch} =
          Enum.reduce(messages, {queue, batch}, fn message, {queue, batch} ->
            if length(batch) + 1 == batch_size do
              {:queue.in(Enum.reverse([message | batch]), queue), []}
            else
              {queue, [message | batch]}
            end
          end)

        {:noreply,
         %{
           state
           | queue: queue,
             batch: batch,
             queue_size: queue_size + length(messages),
             permits: permits + length(dead_letters)
         }}
      end

      def handle_cast(
            {:ack_response, {:ok, %{message_ids: message_ids}}},
            %{pending_acks: pending_acks} = state
          ) do
        Logger.debug("Received #{length(message_ids)} acked message_ids")

        :telemetry.execute(
          [:pulsar_ex, :consumer, :ack_response, :success],
          %{count: length(message_ids)},
          state
        )

        {:noreply, %{state | pending_acks: Map.drop(pending_acks, message_ids)}}
      end

      def handle_cast(
            {:ack_response, {:error, %{message_ids: message_ids, error: err}}},
            %{pending_acks: pending_acks} = state
          ) do
        Logger.error(
          "Received #{length(message_ids)} acked message_ids with error, #{inspect(err)}"
        )

        :telemetry.execute(
          [:pulsar_ex, :consumer, :ack_response, :error],
          %{count: length(message_ids)},
          state
        )

        {:noreply, state}
      end

      # ===================  terminate  ===================
      @impl true
      def terminate(:normal, %{consumer_id: consumer_id, connection: conn} = state) do
        Logger.debug("Stopping consumer")

        if conn do
          connection_module().close_consumer(conn, consumer_id)
        end

        state
      end

      def terminate(:shutdown, %{consumer_id: consumer_id, connection: conn} = state) do
        Logger.debug("Stopping consumer")

        if conn do
          connection_module().close_consumer(conn, consumer_id)
        end

        state
      end

      def terminate({:shutdown, err}, %{consumer_id: consumer_id, connection: conn} = state) do
        Logger.warning("Stopping consumer, #{inspect(err)}")

        if conn do
          connection_module().close_consumer(conn, consumer_id)
        end

        state
      end

      def terminate(reason, %{consumer_id: consumer_id, connection: conn} = state) do
        Logger.error("Exiting consumer, #{inspect(reason)}")

        if conn do
          connection_module().close_consumer(conn, consumer_id)
        end

        :telemetry.execute(
          [:pulsar_ex, :consumer, :exit],
          %{count: 1},
          state
        )

        state
      end

      # ===================  ConsumerCallback  ===================
      @impl ConsumerCallback
      def handle_messages(messages, _state) do
        Enum.map(messages, &Logger.info/1)
      end

      defoverridable handle_messages: 2

      @impl ConsumerCallback
      def send_to_dead_letter(_message, %{dead_letter_topic: nil}), do: :ok

      def send_to_dead_letter(
            %ConsumerMessage{payload: payload} = message,
            %{
              cluster: %Cluster{cluster_name: cluster_name},
              dead_letter_topic: dead_letter_topic,
              dead_letter_producer_opts: dead_letter_producer_opts
            } = _state
          ) do
        message_opts =
          Map.take(message, [:properties, :partition_key, :ordering_key, :event_time])

        PulsarEx.Cluster.produce(
          cluster_name,
          dead_letter_topic,
          payload,
          message_opts,
          dead_letter_producer_opts
        )
      end

      defoverridable send_to_dead_letter: 2

      # ===================  private  ===================
      defp handle_empty(%{batch: []} = state) do
        handle_flow_permits(state)
      end

      defp handle_empty(%{batch: batch} = state) do
        handle_batch(Enum.reverse(batch), %{state | batch: [], queue_size: 0})
      end

      defp handle_batch(batch, %{permits: permits} = state) do
        result =
          try do
            handle_messages(batch, state)
          rescue
            err ->
              Logger.error(Exception.format(:error, err, __STACKTRACE__))

              Logger.error("Error handling batch of #{length(batch)} messages, #{inspect(err)}")

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

        state = %{state | permits: permits + length(batch)}

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
             %{
               permits: permits,
               flow_permits_watermark: flow_permits_watermark,
               poll_interval: poll_interval,
               queue_size: queue_size
             } = state
           )
           when permits < flow_permits_watermark do
        if queue_size > 0 do
          Process.send(self(), :poll, [])
        else
          Process.send_after(self(), :poll, poll_interval)
        end

        {:noreply, state}
      end

      defp handle_flow_permits(
             %{
               connection: conn,
               consumer_id: consumer_id,
               queue_size: queue_size,
               poll_interval: poll_interval,
               flow_permits_sent: flow_permits_sent,
               permits: permits
             } = state
           ) do
        start = System.monotonic_time()

        case connection_module().flow_permits(conn, consumer_id, permits) do
          :ok ->
            Logger.debug("Sent #{permits} permits")

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flow_permits, :success],
              %{count: 1, permits: permits},
              state
            )

            if queue_size > 0 do
              Process.send(self(), :poll, [])
            else
              Process.send_after(self(), :poll, poll_interval)
            end

            {:noreply, %{state | permits: 0, flow_permits_sent: flow_permits_sent + permits}}

          {:error, err} ->
            Logger.error("Error sending #{permits} permits, #{inspect(err)}")

            :telemetry.execute(
              [:pulsar_ex, :consumer, :flow_permits, :error],
              %{count: 1, permits: permits},
              state
            )

            {:stop, {:shutdown, err}, state}
        end
      end

      defp track_ack(
             %{
               message_id: %{ledgerId: ledger_id, entryId: entry_id},
               batch_size: batch_size,
               batch_index: batch_index
             },
             %{acks: acks} = state
           )
           when batch_size > 1 do
        key = {ledger_id, entry_id}

        value =
          case Map.get(acks, key) do
            nil ->
              AckSet.new(batch_size) |> AckSet.set(batch_index)

            {true, batch_size} ->
              {true, batch_size}

            {false, ts, batch_size} ->
              {false, ts, batch_size}

            ack_set ->
              ack_set =
                AckSet.and_set(
                  ack_set,
                  AckSet.new(batch_size) |> AckSet.set(batch_index)
                )

              if AckSet.clear?(ack_set) do
                {true, batch_size}
              else
                ack_set
              end
          end

        acks = Map.put(acks, key, value)
        %{state | acks: acks}
      end

      defp track_ack(
             %{message_id: %{ledgerId: ledger_id, entryId: entry_id}},
             %{acks: acks} = state
           ) do
        key = {ledger_id, entry_id}
        acks = Map.put(acks, key, {true, 1})
        %{state | acks: acks}
      end

      defp track_nack(
             %{
               message_id: %{ledgerId: ledger_id, entryId: entry_id},
               batch_size: batch_size,
               redelivery_count: redelivery_count
             },
             %{
               acks: acks,
               redelivery_policy: redelivery_policy,
               redelivery_interval: redelivery_interval
             } = state
           ) do
        resend_ts =
          case redelivery_policy do
            :exp ->
              System.monotonic_time(:millisecond) + 2 ** redelivery_count * redelivery_interval

            _ ->
              System.monotonic_time(:millisecond) + redelivery_interval
          end

        key = {ledger_id, entry_id}
        acks = Map.put(acks, key, {false, resend_ts, batch_size})
        %{state | acks: acks}
      end

      defp connection_module() do
        Application.get_env(:pulsar_ex, :connection_module, Connection)
      end
    end
  end
end
