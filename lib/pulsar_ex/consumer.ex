defmodule PulsarEx.Consumer do
  @moduledoc false
  use GenServer

  require Logger

  alias PulsarEx.{ConsumerRegistry, Pulserl.Structures}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init([pid, topic, subscription, %{poll_interval: poll_interval} = opts]) do
    Process.send_after(self(), :poll, poll_interval)

    Registry.register(ConsumerRegistry, {topic, subscription}, opts)

    {:ok,
     Map.merge(opts, %{
       pid: pid,
       topic: topic,
       subscription: subscription,
       unprocessed_messages: [],
       last_flushed_at: Timex.now()
     })}
  end

  @impl true
  def handle_info(
        :poll,
        %{
          pid: pid,
          topic: topic,
          subscription: subscription,
          poll_interval: poll_interval,
          unprocessed_messages: unprocessed_messages
        } = state
      ) do
    if can_flush?(state) do
      case flush(state) do
        :ok ->
          Process.send(self(), :poll, [])
          {:noreply, %{state | unprocessed_messages: [], last_flushed_at: Timex.now()}}

        {:error, err} ->
          Logger.error("Error flushing in pulsar consumer #{inspect(err)}", state: state)

          Process.send_after(self(), :poll, poll_interval)
          {:noreply, state}
      end
    else
      case :pulserl.consume(pid, subscription) do
        false ->
          Process.send_after(self(), :poll, poll_interval)
          {:noreply, state}

        {:error, err} ->
          Logger.error("Error receiving message in pulserl consumer #{inspect(err)}", state: state)

          :telemetry.execute(
            [:pulsar, :receive, :error],
            %{count: 1},
            %{topic: topic, subscription: subscription, error: err}
          )

          Process.send_after(self(), :poll, poll_interval)
          {:noreply, state}

        msg ->
          unprocessed_messages = :lists.append(unprocessed_messages, [Structures.to_struct(msg)])

          Process.send(self(), :poll, [])
          {:noreply, %{state | unprocessed_messages: unprocessed_messages}}
      end
    end
  end

  defp can_flush?(%{
         unprocessed_messages: unprocessed_messages,
         batch_size: batch_size,
         poll_interval: poll_interval,
         last_flushed_at: last_flushed_at
       }) do
    length(unprocessed_messages) >= batch_size or
      last_flushed_at
      |> Timex.add(Timex.Duration.from_milliseconds(poll_interval))
      |> Timex.before?(Timex.now())
  end

  defp flush(%{unprocessed_messages: []}), do: :ok

  defp flush(
         %{
           topic: topic,
           subscription: subscription,
           callback_module: callback_module,
           unprocessed_messages: unprocessed_messages
         } = state
       ) do
    try do
      :telemetry.span(
        [:pulsar, :handle_messages],
        %{topic: topic, subscription: subscription},
        fn ->
          # It's ok to send out many ack/nacks because the actual consumer will batch them
          callback_module.handle_messages(unprocessed_messages, state)
          |> Enum.map(fn
            {:ack, %Structures.ConsumerMessage{id: id, consumer: consumer}} ->
              Task.async(fn -> :pulserl.ack(consumer, id) end)

            {:nack, %Structures.ConsumerMessage{id: id, consumer: consumer}} ->
              Task.async(fn -> :pulserl.nack(consumer, id) end)
          end)
          |> Task.await_many()

          :ok
        end
      )
    rescue
      err ->
        {:error, err}
    end
  end
end
