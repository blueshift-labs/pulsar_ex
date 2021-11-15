defmodule PulsarEx.Worker do
  def compile_config(module, opts) do
    {otp_app, opts} = Keyword.pop!(opts, :otp_app)
    opts = Application.get_env(otp_app, module, []) |> Keyword.merge(opts)
    {topic, opts} = Keyword.pop!(opts, :topic)
    {subscription, opts} = Keyword.pop!(opts, :subscription)
    {jobs, opts} = Keyword.pop!(opts, :jobs)
    {middlewares, opts} = Keyword.pop(opts, :middlewares, [])
    {producer_opts, opts} = Keyword.pop(opts, :producer_opts, [])
    {otp_app, topic, subscription, jobs, middlewares, producer_opts, opts}
  end

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      {otp_app, topic, subscription, jobs, middlewares, producer_opts, opts} =
        PulsarEx.Worker.compile_config(__MODULE__, opts)

      opts =
        opts
        |> Keyword.merge(batch_size: 1, initial_position: :earliest)
        |> Keyword.put_new(:dead_letter_topic, topic)
        |> Keyword.put_new(:receiving_queue_size, 10)

      producer_opts =
        producer_opts
        |> Keyword.put_new(:send_timeout, 300_000)

      use PulsarEx.Consumer, opts
      @behaviour PulsarEx.WorkerCallback

      alias PulsarEx.{JobState, ConsumerMessage}

      @otp_app otp_app
      @topic topic
      @subscription subscription
      @jobs jobs
      @default_middlewares [PulsarEx.Middlewares.Telemetry, PulsarEx.Middlewares.Logging]
      @middlewares @default_middlewares ++ middlewares
      @producer_opts producer_opts
      @opts opts

      def topic, do: @topic
      def subscription, do: @subscription
      def subscription_type, do: @subscription_type
      def jobs, do: @jobs

      defp job_handler() do
        handler = fn %JobState{job: job, payload: payload} = job_state ->
          %JobState{job_state | state: handle_job(job, payload)}
        end

        @middlewares
        |> Enum.reverse()
        |> Enum.reduce(handler, fn middleware, acc ->
          middleware.call(acc)
        end)
      end

      @impl true
      def handle_messages([%ConsumerMessage{properties: properties} = message], _state) do
        {job, properties} = Map.pop!(properties, "job")
        payload = Jason.decode!(message.payload)

        job_state =
          job_handler().(%JobState{
            topic: @topic,
            subscription: @subscription,
            job: String.to_atom(job),
            payload: payload,
            properties: properties,
            publish_time: message.publish_time,
            event_time: message.event_time,
            producer_name: message.producer_name,
            partition_key: message.partition_key,
            ordering_key: message.ordering_key,
            deliver_at_time: message.deliver_at_time,
            redelivery_count: message.redelivery_count,
            state: nil
          })

        [job_state.state]
      end

      @impl true
      def handle_job(job, payload) do
        :ok
      end

      defoverridable handle_job: 2

      def enqueue_job(job, params, message_opts \\ []) when job in @jobs do
        start = System.monotonic_time(:millisecond)

        properties =
          Keyword.get(message_opts, :properties, [])
          |> Enum.into(%{})
          |> Map.put("job", job)

        reply =
          PulsarEx.produce(
            @topic,
            Jason.encode!(params),
            Keyword.put(message_opts, :properties, properties),
            @producer_opts
          )

        case reply do
          {:ok, _} ->
            :telemetry.execute(
              [:pulsar_ex, :worker, :enqueue, :success],
              %{count: 1, duration: System.monotonic_time(:millisecond) - start},
              %{topic: @topic, job: job}
            )

          {:error, _} ->
            :telemetry.execute(
              [:pulsar_ex, :worker, :enqueue, :error],
              %{count: 1},
              %{topic: @topic, job: job}
            )
        end

        reply
      end

      def start(opts \\ []) do
        workers = Keyword.get(opts, :workers)

        opts =
          if workers do
            Keyword.merge(@opts, opts) |> Keyword.put(:num_consumers, workers)
          else
            Keyword.merge(@opts, opts)
          end

        PulsarEx.start_consumer(@topic, @subscription, __MODULE__, opts)
      end

      def stop() do
        PulsarEx.stop_consumer(@topic, @subscription)
      end
    end
  end
end
