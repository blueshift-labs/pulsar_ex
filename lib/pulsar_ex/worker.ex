defmodule PulsarEx.Worker do
  def compile_config(module, opts) do
    {otp_app, opts} = Keyword.pop!(opts, :otp_app)
    opts = Application.get_env(otp_app, module, []) |> Keyword.merge(opts)
    {subscription, opts} = Keyword.pop!(opts, :subscription)
    {jobs, opts} = Keyword.pop!(opts, :jobs)
    {inline, opts} = Keyword.pop(opts, :inline, false)
    {middlewares, opts} = Keyword.pop(opts, :middlewares, [])
    {producer_opts, opts} = Keyword.pop(opts, :producer_opts, [])
    {otp_app, subscription, jobs, inline, middlewares, producer_opts, opts}
  end

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      {otp_app, subscription, jobs, inline, middlewares, producer_opts, opts} =
        PulsarEx.Worker.compile_config(__MODULE__, opts)

      opts =
        opts
        |> Keyword.merge(batch_size: 1, initial_position: :earliest)
        |> Keyword.put_new(:dead_letter_topic, :self)
        |> Keyword.put_new(:receiving_queue_size, 10)

      use PulsarEx.Consumer, opts
      @behaviour PulsarEx.WorkerCallback

      alias PulsarEx.{JobState, ConsumerMessage}

      @otp_app otp_app
      @topic Keyword.get(opts, :topic)
      @subscription subscription
      @jobs jobs
      @inline inline
      @default_middlewares [PulsarEx.Middlewares.Telemetry, PulsarEx.Middlewares.Logging]
      @middlewares @default_middlewares ++ middlewares
      @producer_opts producer_opts
      @opts opts

      if @topic do
        def topic(), do: @topic
      else
        def topic(), do: raise("Topic is not defined for #{__MODULE__}")
      end

      def producer_opts(), do: @producer_opts

      defp job_handler() do
        handler = fn %JobState{job: job} = job_state ->
          %JobState{job_state | state: handle_job(job, job_state)}
        end

        @middlewares
        |> Enum.reverse()
        |> Enum.reduce(handler, fn middleware, acc ->
          middleware.call(acc)
        end)
      end

      @impl true
      def handle_messages([%ConsumerMessage{properties: properties} = message], state) do
        {job, properties} = Map.pop!(properties, "job")
        payload = Jason.decode!(message.payload)

        job_state =
          job_handler().(%JobState{
            topic: state.topic_name,
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
      def handle_job(_, _) do
        :ok
      end

      defoverridable handle_job: 2

      defp assert_topic(nil), do: raise("topic undefined")
      defp assert_topic(topic), do: topic

      @impl true
      def topic(_, _, _), do: assert_topic(@topic)

      defoverridable topic: 3

      @impl true
      def partition_key(_, _, message_opts), do: Keyword.get(message_opts, :partition_key)

      defoverridable partition_key: 3

      def enqueue_job(job, params, message_opts \\ [])

      def enqueue_job(job, params, message_opts) do
        {topic, message_opts} =
          Keyword.pop_lazy(message_opts, :topic, fn -> topic(job, params, message_opts) end)

        enqueue_job(job, params, topic, message_opts)
      end

      def enqueue_job(job, params, topic, message_opts) when job in @jobs do
        if @inline do
          handle_job(job, params)
        else
          start = System.monotonic_time()

          properties =
            Keyword.get(message_opts, :properties, [])
            |> Enum.into(%{})
            |> Map.put("job", job)

          reply =
            PulsarEx.produce(
              topic,
              Jason.encode!(params),
              Keyword.merge(message_opts,
                properties: properties,
                partition_key: partition_key(job, params, message_opts)
              ),
              @producer_opts
            )

          case reply do
            {:ok, _} ->
              :telemetry.execute(
                [:pulsar_ex, :worker, :enqueue, :success],
                %{count: 1, duration: System.monotonic_time() - start},
                %{topic: topic, job: job}
              )

            {:error, _} ->
              :telemetry.execute(
                [:pulsar_ex, :worker, :enqueue, :error],
                %{count: 1},
                %{topic: topic, job: job}
              )
          end

          reply
        end
      end

      def start(opts \\ []) do
        workers = Keyword.get(opts, :workers)

        opts =
          if workers do
            Keyword.merge(@opts, opts) |> Keyword.put(:num_consumers, workers)
          else
            Keyword.merge(@opts, opts)
          end

        {topic, opts} = Keyword.pop(opts, :topic)
        {regex, opts} = Keyword.pop(opts, :regex)

        case {topic, regex} do
          {nil, nil} ->
            raise "topic undefined"

          {nil, _} ->
            {tenant, opts} = Keyword.pop!(opts, :tenant)
            {namespace, opts} = Keyword.pop!(opts, :namespace)

            PulsarEx.start_consumer(tenant, namespace, regex, @subscription, __MODULE__, opts)

          {_, nil} ->
            PulsarEx.start_consumer(topic, @subscription, __MODULE__, opts)
        end
      end

      def stop(opts \\ []) do
        opts = Keyword.merge(@opts, opts)

        {topic, opts} = Keyword.pop(opts, :topic)
        {regex, opts} = Keyword.pop(opts, :regex)

        case {topic, regex} do
          {nil, nil} ->
            raise "topic undefined"

          {nil, _} ->
            {tenant, opts} = Keyword.pop!(opts, :tenant)
            {namespace, opts} = Keyword.pop!(opts, :namespace)

            PulsarEx.stop_consumer(tenant, namespace, regex, @subscription)

          {_, nil} ->
            PulsarEx.stop_consumer(topic, @subscription)
        end
      end
    end
  end
end
