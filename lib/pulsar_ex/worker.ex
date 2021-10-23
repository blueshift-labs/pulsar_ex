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
        |> Keyword.merge(batch_size: 1, passive_mode: false, initial_position: :earliest)
        |> Keyword.put_new(:dead_letter_topic, topic)

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
      @timeout 5_000

      def worker_spec(opts) do
        workers = Keyword.get(opts, :workers) || Keyword.get(@opts, :workers)
        timeout = Keyword.get(opts, :timeout) || Keyword.get(@opts, :timeout) || @timeout

        opts =
          case workers do
            nil -> opts
            workers -> Keyword.put(opts, :num_consumers, workers)
          end

        {
          @topic,
          @subscription,
          __MODULE__,
          Keyword.merge(@opts, opts),
          timeout
        }
      end

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
            started_at: nil,
            finished_at: nil,
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
        start = System.monotonic_time()

        properties =
          Keyword.get(message_opts, :properties, [])
          |> Enum.into(%{})
          |> Map.put("job", job)

        reply =
          PulsarEx.sync_produce(
            @topic,
            Jason.encode!(params),
            Keyword.put(message_opts, :properties, properties),
            @producer_opts
          )

        case reply do
          {:ok, _} ->
            :telemetry.execute(
              [:pulsar_ex, :worker, :enqueue, :success],
              %{count: 1, duration: System.monotonic_time() - start},
              %{topic: @topic, job: job}
            )

          {:error, _} ->
            :telemetry.execute(
              [:pulsar_ex, :worker, :enqueue, :error],
              %{count: 1, duration: System.monotonic_time() - start},
              %{topic: @topic, job: job}
            )
        end

        reply
      end

      def enqueue_job_async(job, params, message_opts \\ []) when job in @jobs do
        start = System.monotonic_time()

        properties =
          Keyword.get(message_opts, :properties, [])
          |> Enum.into(%{})
          |> Map.put("job", job)

        reply =
          PulsarEx.async_produce(
            @topic,
            Jason.encode!(params),
            Keyword.put(message_opts, :properties, properties),
            @producer_opts
          )

        case reply do
          {:ok, _} ->
            :telemetry.execute(
              [:pulsar_ex, :worker, :enqueue_async, :success],
              %{count: 1, duration: System.monotonic_time() - start},
              %{topic: @topic, job: job}
            )

          {:error, _} ->
            :telemetry.execute(
              [:pulsar_ex, :worker, :enqueue_async, :error],
              %{count: 1, duration: System.monotonic_time() - start},
              %{topic: @topic, job: job}
            )
        end

        reply
      end
    end
  end
end
