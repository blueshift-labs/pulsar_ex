defmodule PulsarEx.Worker do
  def compile_config(module, opts) do
    {otp_app, opts} = Keyword.pop!(opts, :otp_app)
    opts = Application.fetch_env!(otp_app, module) |> Keyword.merge(opts)
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

      alias PulsarEx.Pulserl.Structures.ConsumerMessage

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
        dead_letter_topic_max_redeliver_count: 5
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
          {:messageId, _, _, _, _, _} -> :ok
          other -> other
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
        case handle_job(job |> String.to_atom(), Jason.decode!(payload)) do
          :ok -> [{:ack, msg}]
          _ -> [{:nack, msg}]
        end
      end
    end
  end
end
