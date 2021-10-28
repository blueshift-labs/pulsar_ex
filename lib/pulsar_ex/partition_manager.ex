defmodule PulsarEx.PartitionManager do
  use GenServer
  alias PulsarEx.{Topic, Admin}

  require Logger

  @table :pulsar_partitions
  @watch_interval 30_000

  def lookup(topic_name) do
    case :ets.lookup(@table, topic_name) do
      [{^topic_name, {topic, partitions}}] ->
        {:ok, {topic, partitions}}

      [] ->
        GenServer.call(__MODULE__, {:lookup, topic_name})
    end
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :init, name: __MODULE__)
  end

  def init(_) do
    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    :ets.new(:pulsar_partitions, [
      :named_table,
      :set,
      :public,
      write_concurrency: false,
      read_concurrency: true
    ])

    {:ok, %{brokers: brokers, admin_port: admin_port}}
  end

  def handle_call(
        {:lookup, topic_name},
        _from,
        %{brokers: brokers, admin_port: admin_port} = state
      ) do
    with [] <- :ets.lookup(@table, topic_name),
         {:ok, %Topic{} = topic} <- Topic.parse(topic_name),
         {:ok, partitions} <- Admin.lookup_topic_partitions(brokers, admin_port, topic) do
      :ets.insert(@table, {topic_name, {topic, partitions}})

      if partitions > 0 do
        Process.send_after(
          self(),
          {:watch, topic_name, topic},
          @watch_interval + :rand.uniform(@watch_interval)
        )
      end

      {:reply, {:ok, {topic, partitions}}, state}
    else
      [{^topic_name, {topic, partitions}}] -> {:reply, {:ok, {topic, partitions}}, state}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  def handle_info(
        {:watch, topic_name, topic},
        %{brokers: brokers, admin_port: admin_port} = state
      ) do
    case Admin.lookup_topic_partitions(brokers, admin_port, topic) do
      {:ok, partitions} ->
        :ets.insert(@table, {topic_name, {topic, partitions}})

      {:error, err} ->
        Logger.error("Error watching topic partitions for #{topic_name}, #{inspect(err)}")
    end

    Process.send_after(
      self(),
      {:watch, topic_name, topic},
      @watch_interval + :rand.uniform(@watch_interval)
    )

    {:noreply, state}
  end
end
