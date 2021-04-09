defmodule PulsarEx.Consumer do
  @moduledoc false
  use GenServer

  alias PulsarEx.{ConsumerRegistry, Pulserl.Structures, Pulserl.Structures.ConsumerMessage}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init([pid, topic, subscription, %{poll_interval: poll_interval} = opts]) do
    Process.flag(:trap_exit, true)

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
          subscription: subscription,
          poll_interval: poll_interval,
          unprocessed_messages: unprocessed_messages
        } = state
      ) do
    cond do
      can_flush?(state) ->
        case flush(state) do
          :ok ->
            Process.send(self(), :poll, [])

            {:noreply, %{state | unprocessed_messages: [], last_flushed_at: Timex.now()}}

          {:error, err} ->
            IO.inspect(err)
            Process.send_after(self(), :poll, poll_interval)

            {:noreply, state}
        end

      true ->
        case :pulserl.consume(pid, subscription) do
          false ->
            Process.send_after(self(), :poll, poll_interval)
            {:noreply, state}

          {:error, err} ->
            IO.inspect(err)
            Process.send_after(self(), :poll, poll_interval)
            {:noreply, state}

          msg ->
            unprocessed_messages =
              :lists.append(unprocessed_messages, [Structures.to_struct(msg)])

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
           callback_module: callback_module,
           unprocessed_messages: unprocessed_messages
         } = state
       ) do
    try do
      callback_module.handle_messages(unprocessed_messages, state)
      |> Enum.map(fn
        {%Structures.ConsumerMessage{id: id, consumer: consumer}, :ack} ->
          :pulserl.ack(consumer, id)

        {%Structures.ConsumerMessage{id: id, consumer: consumer}, :nack} ->
          :pulserl.nack(consumer, id)
      end)

      :ok
    rescue
      err ->
        {:error, err}
    end
  end

  @impl true
  def terminate(_reason, state) do
    # we don't flush the messages, assuming they will be redelivered by broker
    state
  end
end
