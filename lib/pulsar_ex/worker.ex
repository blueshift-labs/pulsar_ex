defmodule PulsarEx.Worker do
  defmodule EnqueueTimeoutError do
    defexception message: "timeout enqueuing job"
  end

  def compile_config(module, opts) do
    {otp_app, opts} = Keyword.pop!(opts, :otp_app)
    opts = Application.get_env(otp_app, module, []) |> Keyword.merge(opts)
    {topic, opts} = Keyword.pop!(opts, :topic)
    {subscription, opts} = Keyword.pop!(opts, :subscription)
    {jobs, opts} = Keyword.pop!(opts, :jobs)
    {middlewares, opts} = Keyword.pop(opts, :middlewares, [])
    {otp_app, topic, subscription, jobs, middlewares, opts}
  end

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use GenServer
      @behaviour PulsarEx.ConsumerCallback
      @behaviour PulsarEx.WorkerCallback

      alias Pulserl.Header.Structures.ConsumerMessage
      alias PulsarEx.JobState

      require Logger

      {otp_app, topic, subscription, jobs, middlewares, worker_opts} =
        PulsarEx.Worker.compile_config(__MODULE__, opts)

      @default_middlewares [PulsarEx.Middlewares.Telemetry, PulsarEx.Middlewares.Logging]

      @middlewares @default_middlewares ++ middlewares

      @otp_app otp_app
      @topic topic
      @subscription subscription
      @jobs jobs
      @worker_opts worker_opts
      @default_opts [
        # by default, we batch the acks
        acknowledgment_timeout: 1_000,
        # by default, we send the message back to end of the topic
        dead_letter_topic_name: topic,
        dead_letter_topic_max_redeliver_count: 5,
        # using pulsar as a message queue expects each job to take longer, while more consumers to distribute the load
        queue_size: 10
      ]

      def job_handler() do
        handler = fn %JobState{job: job, payload: payload} = job_state ->
          %JobState{job_state | state: handle_job(job, payload)}
        end

        @middlewares
        |> Enum.reverse()
        |> Enum.reduce(handler, fn middleware, acc ->
          middleware.call(acc)
        end)
      end

      @spec enqueue_job(job :: atom(), params :: map(), message_opts :: keyword()) ::
              :ok | {:error, :timeout}
      def enqueue_job(job, params, message_opts \\ []) when job in @jobs do
        properties =
          Keyword.get(message_opts, :properties, [])
          |> Enum.into(%{})
          |> Map.put("job", job)

        PulsarEx.produce(
          @topic,
          Jason.encode!(params),
          Keyword.put(message_opts, :properties, properties)
        )
        |> case do
          {:messageId, _, _, _, _, _} ->
            :telemetry.execute(
              [:pulsar, :worker, :enqueue, :success, :count],
              %{count: 1},
              %{job: job, topic: @topic}
            )

            :ok

          {:error, :timeout} ->
            :telemetry.execute(
              [:pulsar, :worker, :enqueue, :timeout, :count],
              %{count: 1},
              %{job: job, topic: @topic}
            )

            {:error, :timeout}
        end
      end

      @spec enqueue_job!(job :: atom(), params :: map(), message_opts :: keyword()) ::
              :ok | {:error, :timeout}
      def enqueue_job!(job, params, message_opts \\ []) when job in @jobs do
        case enqueue_job(job, params, message_opts) do
          :ok ->
            :ok

          {:error, :timeout} ->
            raise PulsarEx.Worker.EnqueueTimeoutError
        end
      end

      def start_link(opts) do
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
      end

      def init(opts) do
        Process.flag(:trap_exit, true)

        opts =
          @default_opts
          |> Keyword.merge(@worker_opts)
          |> Keyword.merge(opts ++ [batch_size: 1, initial_position: :earliest])

        PulsarEx.start_consumers(@topic, @subscription, __MODULE__, opts)
        {:ok, opts}
      end

      def handle_info({:EXIT, _pid, :normal}, state) do
        {:noreply, state}
      end

      def terminate(reason, state) do
        PulsarEx.stop_consumers(@topic, @subscription)
        state
      end

      @impl PulsarEx.ConsumerCallback
      def handle_messages(
            [%ConsumerMessage{properties: %{"job" => job}, payload: payload} = msg],
            _state
          ) do

        job_state =
          job_handler().(%JobState{
            topic: @topic,
            subscription: @subscription,
            job: String.to_atom(job),
            payload: Jason.decode!(payload)
          })

        case job_state do
          %JobState{state: :ok} ->
            [{:ack, msg}]

          _ ->
            [{:nack, msg}]
        end
      end
    end
  end
end
