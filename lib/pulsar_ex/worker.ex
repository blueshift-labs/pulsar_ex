defmodule PulsarEx.Worker do
  def compile_config(module, opts) do
    {otp_app, opts} = Keyword.pop!(opts, :otp_app)
    opts = Application.get_env(otp_app, module, []) |> Keyword.merge(opts)
    {topic, opts} = Keyword.pop!(opts, :topic)
    {subscription, opts} = Keyword.pop!(opts, :subscription)
    {jobs, opts} = Keyword.pop!(opts, :jobs)
    {otp_app, topic, subscription, jobs, opts}
  end

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use GenServer
      @behaviour PulsarEx.ConsumerCallback
      @behaviour PulsarEx.Worker.Callback

      alias Pulserl.Header.Structures.ConsumerMessage

      {otp_app, topic, subscription, jobs, worker_opts} =
        PulsarEx.Worker.compile_config(__MODULE__, opts)

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

      def handle_messages(
            [%ConsumerMessage{properties: %{"job" => job}, payload: payload} = msg],
            _state
          ) do
        job = String.to_atom(job)
        metadata = %{job: job, topic: @topic, subscription: @subscription}

        :telemetry.span(
          [:pulsar, :handle_job],
          metadata,
          fn ->
            case handle_job(job, Jason.decode!(payload)) do
              :ok ->
                :telemetry.execute(
                  [:pulsar, :handle_job, :success, :count],
                  %{count: 1},
                  metadata
                )

                {[{:ack, msg}], metadata}

              _ ->
                :telemetry.execute(
                  [:pulsar, :handle_job, :error, :count],
                  %{count: 1},
                  metadata
                )

                {[{:nack, msg}], metadata}
            end
          end
        )
      end
    end
  end
end
