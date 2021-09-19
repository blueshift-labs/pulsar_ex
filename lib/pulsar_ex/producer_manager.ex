defmodule PulsarEx.ProducerManager do
  use GenServer

  require Logger

  alias PulsarEx.{Producers, PartitionedProducerSupervisor}

  def create(topic_name, opts) do
    GenServer.call(__MODULE__, {:create, topic_name, opts})
  end

  def start_link({lookup, auto_start}) do
    GenServer.start_link(__MODULE__, {lookup, auto_start}, name: __MODULE__)
  end

  @impl true
  def init({lookup, false}) do
    Logger.debug("Starting producer manager")

    {:ok, %{producers: %{}, lookup: lookup}}
  end

  @impl true
  def init({lookup, true}) do
    Logger.debug("Starting producer manager")

    {err, producers} =
      Application.get_env(:pulsar_ex, :producers, [])
      |> Enum.reduce_while({nil, %{}}, fn opts, {nil, started} ->
        topic_name = Keyword.fetch!(opts, :topic)
        producer_opts = producer_opts(opts)

        case start_producer(topic_name, producer_opts, lookup) do
          :ok -> {:cont, {nil, Map.put(started, topic_name, producer_opts)}}
          {:error, _} = err -> {:halt, {err, started}}
        end
      end)

    case err do
      nil -> {:ok, %{producers: producers, lookup: lookup}}
      _ -> {:stop, err}
    end
  end

  @impl true
  def handle_call(
        {:create, topic_name, opts},
        _from,
        %{producers: producers, lookup: lookup} = state
      ) do
    cond do
      Map.has_key?(producers, topic_name) ->
        {:reply, {:error, :already_started}, state}

      true ->
        producer_opts = producer_opts(opts)

        case start_producer(topic_name, producer_opts, lookup) do
          :ok ->
            producers = Map.put(producers, topic_name, producer_opts)
            {:reply, :ok, %{state | producers: producers}}

          {:error, _} = err ->
            {:reply, err, state}
        end
    end
  end

  defp start_producer(topic_name, producer_opts, lookup) do
    Logger.debug("Starting producer for topic #{topic_name}")

    started =
      DynamicSupervisor.start_child(
        Producers,
        {PartitionedProducerSupervisor, {topic_name, producer_opts, lookup}}
      )

    case started do
      {:ok, _} ->
        Logger.debug("Started producer for topic #{topic_name}")
        :ok

      {:error, err} ->
        Logger.error("Error starting producer for topic #{topic_name}, #{inspect(err)}")
        {:error, err}
    end
  end

  defp producer_opts(opts) do
    Keyword.merge(
      Application.get_env(:pulsar_ex, :producer_opts, []),
      opts,
      fn
        :properties, v1, v2 -> Keyword.merge(v1, v2)
        _, _, v -> v
      end
    )
  end
end
