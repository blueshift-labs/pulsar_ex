defmodule PulsarEx.Consumer do
  @moduledoc false
  use GenServer

  require Logger

  alias PulsarEx.ConsumerRegistry
  alias Pulserl.Header.Structures

  @ack_timeout 60_000

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init([topic, subscription, opts, %{poll_interval: poll_interval} = worker_opts]) do
    Process.send_after(self(), :poll, poll_interval)

    Registry.register(ConsumerRegistry, {topic, subscription}, {opts, worker_opts})

    {:ok,
     %{
       topic: topic,
       subscription: subscription,
       unprocessed_messages: [],
       last_flushed_at: Timex.now(),
       opts: opts,
       worker_opts: worker_opts
     }}
  end

  @impl true
  def handle_info(
        :poll,
        %{
          topic: topic,
          subscription: subscription,
          unprocessed_messages: unprocessed_messages,
          opts: opts,
          worker_opts: %{
            poll_interval: poll_interval,
            poll_size: poll_size
          }
        } = state
      ) do
    if can_flush?(state) do
      case flush(state) do
        {:ok, rest} ->
          Process.send(self(), :poll, [])
          {:noreply, %{state | unprocessed_messages: rest, last_flushed_at: Timex.now()}}

        {:error, _} ->
          Process.send_after(self(), :poll, poll_interval)
          {:noreply, state}
      end
    else
      with {:ok, consumer} <- :pulserl_instance_registry.get_consumer(topic, subscription, opts),
           msgs when is_list(msgs) <- :pulserl.consume(consumer, subscription, poll_size) do
        unprocessed_messages =
          :lists.append(unprocessed_messages, Enum.map(msgs, &Structures.to_struct/1))

        Process.send(self(), :poll, [])
        {:noreply, %{state | unprocessed_messages: unprocessed_messages}}
      else
        false ->
          Process.send_after(self(), :poll, poll_interval)
          {:noreply, state}

        err ->
          Logger.error("Error receiving message in pulserl consumer #{inspect(err)}", state: state)

          :telemetry.execute(
            [:pulsar_ex, :receive, :error],
            %{count: 1},
            %{topic: topic, subscription: subscription}
          )

          Process.send_after(self(), :poll, poll_interval)
          {:noreply, state}
      end
    end
  end

  defp can_flush?(%{
         unprocessed_messages: unprocessed_messages,
         last_flushed_at: last_flushed_at,
         worker_opts: %{
           batch_size: batch_size,
           poll_interval: poll_interval
         }
       }) do
    length(unprocessed_messages) >= batch_size or
      last_flushed_at
      |> Timex.add(Timex.Duration.from_milliseconds(poll_interval))
      |> Timex.before?(Timex.now())
  end

  defp flush(%{unprocessed_messages: []}), do: {:ok, []}

  defp flush(
         %{
           topic: topic,
           subscription: subscription,
           unprocessed_messages: unprocessed_messages,
           worker_opts: %{
             batch_size: batch_size,
             callback_module: callback_module
           }
         } = state
       ) do
    try do
      start = System.monotonic_time()
      metadata = %{topic: topic, subscription: subscription}

      {batch, rest} = Enum.split(unprocessed_messages, batch_size)

      acks =
        callback_module.handle_messages(batch, state)
        |> Task.async_stream(
          fn
            {:ack, %Structures.ConsumerMessage{id: id, consumer: consumer}} ->
              :pulserl.ack(consumer, id)

            {:nack, %Structures.ConsumerMessage{id: id, consumer: consumer}} ->
              :pulserl.nack(consumer, id)
          end,
          on_timeout: :kill_task,
          timeout: @ack_timeout
        )
        |> Enum.count(&match?({:ok, :ok}, &1))

      :telemetry.execute(
        [:pulsar_ex, :handle_messages, :failed_acks],
        %{count: length(batch) - acks},
        metadata
      )

      :telemetry.execute(
        [:pulsar_ex, :handle_messages],
        %{count: length(batch), duration: System.monotonic_time() - start},
        metadata
      )

      {:ok, rest}
    rescue
      err ->
        Logger.error("Error flushing in pulsar consumer #{inspect(err)}", state: state)
        Logger.error("Stacktrace #{inspect(__STACKTRACE__)}")
        {:error, err}
    end
  end
end
