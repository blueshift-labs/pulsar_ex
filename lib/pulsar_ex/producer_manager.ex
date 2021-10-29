defmodule PulsarEx.ProducerManager do
  use GenServer

  require Logger

  alias PulsarEx.{Producers, ProducerRegistry, PartitionedProducer, PartitionManager, Topic}

  @num_producers 1

  def get_producer(topic_name, partition, opts \\ []) do
    with [] <- Registry.lookup(ProducerRegistry, {topic_name, partition}),
         {:ok, pool} <- GenServer.call(__MODULE__, {:create, topic_name, partition, opts}) do
      {:ok, :poolboy.transaction(pool, & &1)}
    else
      [{pool, _}] ->
        {:ok, :poolboy.transaction(pool, & &1)}

      err ->
        err
    end
  end

  def start_link(auto_start) do
    GenServer.start_link(__MODULE__, auto_start, name: __MODULE__)
  end

  @impl true
  def init(false) do
    Logger.debug("Starting producer manager")
    {:ok, nil}
  end

  @impl true
  def init(true) do
    Logger.debug("Starting producer manager")

    Application.get_env(:pulsar_ex, :producers, [])
    |> Enum.reduce_while(:ok, fn opts, :ok ->
      topic_name = Keyword.fetch!(opts, :topic)
      producer_opts = producer_opts(opts)

      case start_producers(topic_name, producer_opts) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      :ok -> {:ok, nil}
      err -> {:stop, err}
    end
  end

  @impl true
  def handle_call({:create, topic_name, partition, opts}, _from, nil) do
    with [] <- Registry.lookup(ProducerRegistry, {topic_name, partition}),
         {:ok, pool} <- start_producer(topic_name, partition, producer_opts(opts)) do
      {:reply, {:ok, pool}, nil}
    else
      [{pool, _}] ->
        {:reply, {:ok, pool}, nil}

      err ->
        {:reply, err, nil}
    end
  end

  defp start_producer(topic_name, partition, producer_opts) do
    started =
      DynamicSupervisor.start_child(
        Producers,
        producer_spec(topic_name, partition, producer_opts)
      )

    case started do
      {:ok, _} ->
        Logger.debug("Started producer for topic #{topic_name}, partition #{partition}")

      {:error, err} ->
        Logger.error(
          "Error starting producer for topic #{topic_name}, partition #{partition}, #{
            inspect(err)
          }"
        )
    end

    started
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

  defp producer_spec(topic_name, partition, producer_opts) do
    {
      {topic_name, partition},
      {
        :poolboy,
        :start_link,
        [
          [
            name: {:via, Registry, {ProducerRegistry, {topic_name, partition}}},
            worker_module: PartitionedProducer,
            size: Keyword.get(producer_opts, :num_producers, @num_producers),
            max_overflow: 0,
            strategy: :fifo
          ],
          {topic_name, partition, producer_opts}
        ]
      },
      :permanent,
      :infinity,
      :supervisor,
      [:poolboy, PartitionedProducer]
    }
  end

  defp start_producers(topic_name, producer_opts) do
    with {:ok, {%Topic{partition: nil}, partitions}} <- PartitionManager.lookup(topic_name) do
      partitions = if partitions > 0, do: 0..(partitions - 1), else: [nil]

      partitions
      |> Enum.reduce_while(:ok, fn partition, :ok ->
        start_producer(topic_name, partition, producer_opts)
        |> case do
          {:ok, _} -> {:cont, :ok}
          err -> {:halt, err}
        end
      end)
    end
  end
end
