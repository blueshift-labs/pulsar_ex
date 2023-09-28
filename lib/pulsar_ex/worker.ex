defmodule PulsarEx.Worker do
  def compile_config(module, opts) do
    {otp_app, opts} = Keyword.pop!(opts, :otp_app)
    opts = Application.get_env(otp_app, module, []) |> Keyword.merge(opts)
    {cluster, opts} = Keyword.pop(opts, :cluster, "default")
    {jobs, opts} = Keyword.pop(opts, :jobs, [])
    {inline, opts} = Keyword.pop(opts, :inline, false)
    {interceptors, opts} = Keyword.pop(opts, :interceptors, [])
    {middlewares, opts} = Keyword.pop(opts, :middlewares, [])
    {producer_opts, opts} = Keyword.pop(opts, :producer_opts, [])

    {otp_app, cluster, jobs, inline, interceptors, middlewares, producer_opts, opts}
  end

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      {otp_app, cluster, jobs, inline, interceptors, middlewares, producer_opts, opts} =
        PulsarEx.Worker.compile_config(__MODULE__, opts)

      require Logger

      if Keyword.get(opts, :batch_enabled) do
        Logger.warn(
          "Workers should not be configured with batch_enabled, ignoring batch settings. #{inspect(opts)}"
        )
      end

      opts =
        opts
        |> Keyword.merge(batch_size: 1, initial_position: :earliest)
        |> Keyword.merge(batch_enabled: false)
        |> Keyword.put_new(:dead_letter_topic, :self)
        |> Keyword.put_new(:receiving_queue_size, 10)

      use PulsarEx.Consumer, opts
      @behaviour PulsarEx.WorkerCallback

      alias PulsarEx.{JobInfo, JobState, ConsumerMessage, Cluster, Topic}
      alias PulsarEx.{ConsumerCallback, WorkerCallback}

      @otp_app otp_app
      @cluster cluster
      @topic Keyword.get(opts, :topic)
      @subscription Keyword.get(opts, :subscription)
      @jobs jobs
      @inline inline
      @default_middlewares [PulsarEx.Middlewares.Telemetry, PulsarEx.Middlewares.Logging]
      @interceptors interceptors
      @middlewares @default_middlewares ++ middlewares
      @producer_opts producer_opts
      @opts opts

      def cluster() do
        @cluster
      end

      def jobs() do
        @jobs
      end

      def opts() do
        @opts
      end

      if @topic do
        def topic(), do: @topic
      else
        def topic(), do: raise("Topic is not defined for #{__MODULE__}")
      end

      if @subscription do
        def subscription(), do: "#{@subscription}"
      else
        def subscription(), do: raise("Subscription is not defined for #{__MODULE__}")
      end

      def producer_opts(), do: @producer_opts

      defp job_handler() do
        handler = fn
          %JobState{job: nil} = job_state ->
            %JobState{job_state | state: handle_job(job_state)}

          %JobState{job: job} = job_state ->
            %JobState{job_state | state: handle_job(job, job_state)}
        end

        @middlewares
        |> Enum.reverse()
        |> Enum.reduce(handler, fn middleware, acc ->
          middleware.call(acc)
        end)
      end

      @impl ConsumerCallback
      def handle_messages(
            [%ConsumerMessage{properties: properties} = message],
            %{
              cluster: %Cluster{cluster_name: cluster_name},
              topic: %Topic{} = topic,
              subscription: subscription,
              consumer_opts: consumer_opts
            } = state
          ) do
        {_worker, properties} = Map.pop(properties, "worker")
        {job, properties} = Map.pop(properties, "job")
        job = if job, do: String.to_atom(job), else: nil
        job = if job in @jobs, do: job, else: nil

        payload = Jason.decode!(message.payload)

        handler = fn ->
          job_handler().(%JobState{
            cluster: cluster_name,
            worker: __MODULE__,
            topic: to_string(topic),
            subscription: subscription,
            job: job,
            payload: payload,
            properties: properties,
            publish_time: message.publish_time,
            event_time: message.event_time,
            producer_name: message.producer_name,
            partition_key: message.partition_key,
            ordering_key: message.ordering_key,
            deliver_at_time: message.deliver_at_time,
            redelivery_count: message.redelivery_count,
            consumer_opts: consumer_opts,
            assigns: %{},
            state: nil
          })
        end

        job_state = handler.()

        [job_state.state]
      end

      @impl ConsumerCallback
      def send_to_dead_letter(
            %ConsumerMessage{properties: properties} = message,
            state
          ) do
        super(%{message | properties: Map.put_new(properties, "worker", __MODULE__)}, state)
      end

      @impl WorkerCallback
      def handle_job(_) do
        :ok
      end

      defoverridable handle_job: 1

      @impl WorkerCallback
      def handle_job(_, _) do
        :ok
      end

      defoverridable handle_job: 2

      @impl WorkerCallback
      def cluster(_, _), do: nil

      defoverridable cluster: 2

      @impl WorkerCallback
      def cluster(_, _, _), do: nil

      defoverridable cluster: 3

      defp assert_topic(nil), do: raise("topic undefined")
      defp assert_topic(topic), do: topic

      @impl WorkerCallback
      def topic(_, _), do: assert_topic(@topic)

      defoverridable topic: 2

      @impl WorkerCallback
      def topic(_, _, _), do: assert_topic(@topic)

      defoverridable topic: 3

      @impl WorkerCallback
      def partition_key(_, message_opts), do: Keyword.get(message_opts, :partition_key)

      defoverridable partition_key: 2

      @impl WorkerCallback
      def partition_key(_, _, message_opts), do: Keyword.get(message_opts, :partition_key)

      defoverridable partition_key: 3

      @impl WorkerCallback
      def ordering_key(_, message_opts), do: Keyword.get(message_opts, :ordering_key)

      defoverridable ordering_key: 2

      @impl WorkerCallback
      def ordering_key(_, _, message_opts), do: Keyword.get(message_opts, :ordering_key)

      defoverridable ordering_key: 3

      if @jobs == [] do
        def enqueue(params, message_opts \\ [])

        def enqueue(params, message_opts) do
          {topic, message_opts} =
            Keyword.pop_lazy(message_opts, :topic, fn -> topic(params, message_opts) end)

          {cluster, message_opts} =
            Keyword.pop_lazy(message_opts, :cluster, fn ->
              cluster(params, message_opts) || cluster()
            end)

          {partition_key, message_opts} =
            Keyword.pop_lazy(message_opts, :partition_key, fn ->
              partition_key(params, message_opts)
            end)

          {ordering_key, message_opts} =
            Keyword.pop_lazy(message_opts, :ordering_key, fn ->
              ordering_key(params, message_opts)
            end)

          do_enqueue_job(nil, params, topic, cluster, partition_key, ordering_key, message_opts)
        end

        def enqueue(params, topic, message_opts) do
          message_opts = Keyword.merge(message_opts, topic: topic, cluster: :default)
          enqueue(params, message_opts)
        end

        def enqueue(params, topic, cluster, message_opts) do
          message_opts = Keyword.merge(message_opts, topic: topic, cluster: cluster)
          enqueue(params, message_opts)
        end
      else
        def enqueue_job(job, params, message_opts \\ [])

        def enqueue_job(job, params, message_opts) when job in @jobs do
          {topic, message_opts} =
            Keyword.pop_lazy(message_opts, :topic, fn -> topic(job, params, message_opts) end)

          {cluster, message_opts} =
            Keyword.pop_lazy(message_opts, :cluster, fn ->
              cluster(job, params, message_opts) || cluster()
            end)

          {partition_key, message_opts} =
            Keyword.pop_lazy(message_opts, :partition_key, fn ->
              partition_key(job, params, message_opts)
            end)

          {ordering_key, message_opts} =
            Keyword.pop_lazy(message_opts, :ordering_key, fn ->
              ordering_key(job, params, message_opts)
            end)

          do_enqueue_job(job, params, topic, cluster, partition_key, ordering_key, message_opts)
        end

        def enqueue_job(job, params, topic, message_opts) do
          message_opts = Keyword.merge(message_opts, topic: topic, cluster: :default)
          enqueue_job(job, params, message_opts)
        end

        def enqueue_job(job, params, topic, cluster, message_opts) do
          message_opts = Keyword.merge(message_opts, topic: topic, cluster: cluster)
          enqueue_job(job, params, message_opts)
        end
      end

      defp inline(
             %JobInfo{
               cluster: cluster,
               worker: __MODULE__,
               topic: topic,
               job: job,
               params: params,
               partition_key: partition_key,
               ordering_key: ordering_key,
               message_opts: message_opts
             } = job_info
           ) do
        properties =
          message_opts
          |> Keyword.get(:properties, [])
          |> Enum.map(fn {k, v} -> {"#{k}", "#{v}"} end)
          |> Enum.into(%{})

        subscription = if @subscription, do: "#{@subscription}", else: nil

        %JobState{state: state} =
          job_handler().(%JobState{
            cluster: cluster,
            worker: __MODULE__,
            topic: topic,
            subscription: subscription,
            job: job,
            payload: params,
            properties: properties,
            publish_time: Timex.now(),
            event_time: nil,
            producer_name: "inline",
            partition_key: partition_key,
            ordering_key: ordering_key,
            deliver_at_time: nil,
            redelivery_count: 0,
            consumer_opts: nil,
            assigns: %{},
            state: nil
          })

        state =
          case state do
            :ok -> {:ok, nil}
            _ -> state
          end

        %{job_info | state: state}
      end

      defp outline(
             %JobInfo{
               cluster: cluster,
               worker: worker,
               topic: topic,
               job: job,
               params: params,
               partition_key: partition_key,
               ordering_key: ordering_key,
               message_opts: message_opts
             } = job_info
           ) do
        start = System.monotonic_time()

        metadata =
          if job do
            %{cluster: cluster, topic: topic, job: job}
          else
            %{cluster: cluster, topic: topic}
          end

        properties =
          Keyword.get(message_opts, :properties, [])
          |> Enum.into(%{})

        properties =
          if job do
            Map.merge(properties, %{"job" => job, "worker" => worker})
          else
            Map.put(properties, "worker", worker)
          end

        message_opts =
          Keyword.merge(message_opts,
            properties: properties,
            partition_key: partition_key,
            ordering_key: ordering_key
          )
          |> Enum.reject(&match?({_, nil}, &1))

        state =
          PulsarEx.Cluster.produce(
            cluster,
            topic,
            Jason.encode!(params),
            message_opts,
            @producer_opts
          )

        case state do
          {:ok, _} ->
            :telemetry.execute(
              [:pulsar_ex, :worker, :enqueue, :success],
              %{count: 1, duration: System.monotonic_time() - start},
              metadata
            )

          {:error, _} ->
            :telemetry.execute(
              [:pulsar_ex, :worker, :enqueue, :error],
              %{count: 1},
              metadata
            )
        end

        %{job_info | state: state}
      end

      # job, partition_key, ordering_key can be nil
      defp do_enqueue_job(job, params, topic, cluster, partition_key, ordering_key, message_opts) do
        params = Jason.decode!(Jason.encode!(params))

        handler = if @inline, do: &inline/1, else: &outline/1

        handler =
          @interceptors
          |> Enum.reverse()
          |> Enum.reduce(handler, fn interceptor, acc ->
            interceptor.call(acc)
          end)

        %JobInfo{state: state} =
          %JobInfo{
            cluster: cluster,
            worker: __MODULE__,
            topic: topic,
            job: job,
            params: params,
            partition_key: partition_key,
            ordering_key: ordering_key,
            message_opts: message_opts,
            assigns: %{},
            state: nil
          }
          |> handler.()

        state
      end

      def start(opts \\ []) do
        workers = Keyword.get(opts, :workers)

        opts =
          if workers do
            Keyword.merge(@opts, opts) |> Keyword.put(:num_consumers, workers)
          else
            Keyword.merge(@opts, opts)
          end

        {cluster, opts} = Keyword.pop(opts, :cluster, cluster())
        {subscription, opts} = Keyword.pop_lazy(opts, :subscription, &subscription/0)
        subscription = "#{subscription}"

        {tenant, opts} = Keyword.pop(opts, :tenant)
        {topic, opts} = Keyword.pop!(opts, :topic)

        unless fully_quantified_topic?(topic) do
          {namespace, opts} = Keyword.pop!(opts, :namespace)

          PulsarEx.Cluster.start_consumer(
            cluster,
            tenant,
            namespace,
            topic,
            subscription,
            __MODULE__,
            opts
          )
        else
          PulsarEx.Cluster.start_consumer(cluster, topic, subscription, __MODULE__, opts)
        end
      end

      def stop(opts \\ []) do
        opts = Keyword.merge(@opts, opts)

        {cluster, opts} = Keyword.pop(opts, :cluster, cluster())
        {subscription, opts} = Keyword.pop_lazy(opts, :subscription, &subscription/0)
        subscription = "#{subscription}"

        {tenant, opts} = Keyword.pop(opts, :tenant)
        {topic, opts} = Keyword.pop!(opts, :topic)

        unless fully_quantified_topic?(topic) do
          {namespace, opts} = Keyword.pop!(opts, :namespace)
          PulsarEx.Cluster.stop_consumer(cluster, tenant, namespace, topic, subscription)
        else
          PulsarEx.Cluster.stop_consumer(cluster, topic, subscription)
        end
      end

      defp fully_quantified_topic?("persistent://" <> _), do: true
      defp fully_quantified_topic?("non-persistent://" <> _), do: true
      defp fully_quantified_topic?(_), do: false
    end
  end
end
