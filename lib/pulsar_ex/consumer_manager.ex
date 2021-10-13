defmodule PulsarEx.ConsumerManager do
  use GenServer

  require Logger

  alias PulsarEx.{Consumers, PartitionedConsumerSupervisor, DefaultPassiveConsumer}

  def create({topic_name, subscription, module, opts, timeout}) do
    create(topic_name, subscription, module, opts, timeout)
  end

  def create({topic_name, subscription, module, opts}) do
    create(topic_name, subscription, module, opts, 5_000)
  end

  def create(topic_name, subscription, module, opts, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:create, topic_name, subscription, module, opts}, timeout)
  end

  def start_link({lookup, auto_start}) do
    GenServer.start_link(__MODULE__, {lookup, auto_start}, name: __MODULE__)
  end

  @impl true
  def init({lookup, false}) do
    Logger.debug("Starting consumer manager")

    {:ok, %{consumers: %{}, lookup: lookup}}
  end

  @impl true
  def init({lookup, true}) do
    Logger.debug("Starting consumer manager")

    {err, consumers} =
      Application.get_env(:pulsar_ex, :consumers, [])
      |> Enum.reduce_while({nil, %{}}, fn opts, {nil, started} ->
        topic_name = Keyword.fetch!(opts, :topic)
        subscription = Keyword.fetch!(opts, :subscription)
        module = Keyword.get(opts, :module, DefaultPassiveConsumer)
        consumer_opts = consumer_opts(opts)

        case start_consumer(topic_name, subscription, module, consumer_opts, lookup) do
          :ok ->
            {:cont, {nil, Map.put(started, {topic_name, subscription}, {module, consumer_opts})}}

          {:error, _} = err ->
            {:halt, {err, started}}
        end
      end)

    case err do
      nil -> {:ok, %{consumers: consumers, lookup: lookup}}
      _ -> {:stop, err}
    end
  end

  @impl true
  def handle_call(
        {:create, topic_name, subscription, module, opts},
        _from,
        %{consumers: consumers, lookup: lookup} = state
      ) do
    cond do
      Map.has_key?(consumers, {topic_name, subscription}) ->
        {:reply, {:error, :already_started}, state}

      true ->
        consumer_opts = consumer_opts(opts)

        case start_consumer(topic_name, subscription, module, consumer_opts, lookup) do
          :ok ->
            consumers = Map.put(consumers, {topic_name, subscription}, {module, consumer_opts})
            {:reply, :ok, %{state | consumers: consumers}}

          {:error, _} = err ->
            {:reply, err, state}
        end
    end
  end

  defp start_consumer(topic_name, subscription, module, consumer_opts, lookup) do
    Logger.debug("Starting consumer for topic #{topic_name} with subscription #{subscription}")

    started =
      DynamicSupervisor.start_child(
        Consumers,
        {PartitionedConsumerSupervisor, {topic_name, subscription, module, consumer_opts, lookup}}
      )

    case started do
      {:ok, _} ->
        Logger.debug("Started consumer for topic #{topic_name} with subscription #{subscription}")
        :ok

      {:error, err} ->
        Logger.error(
          "Error starting consumer for topic #{topic_name} with subscription #{subscription}, #{
            inspect(err)
          }"
        )

        {:error, err}
    end
  end

  defp consumer_opts(opts) do
    Keyword.merge(
      Application.get_env(:pulsar_ex, :consumer_opts, []),
      opts,
      fn
        :properties, v1, v2 -> Keyword.merge(v1, v2)
        _, _, v -> v
      end
    )
  end
end
