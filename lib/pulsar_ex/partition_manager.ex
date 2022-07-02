defmodule PulsarEx.PartitionManager do
  defmodule State do
    @enforce_keys [
      :cluster,
      :cluster_opts,
      :brokers,
      :admin_port
    ]

    defstruct [
      :cluster,
      :cluster_opts,
      :brokers,
      :admin_port
    ]
  end

  use GenServer
  alias PulsarEx.{Topic, Admin}

  require Logger

  @watch_interval 30_000

  def lookup(cluster, topic_name) do
    case :ets.lookup(PulsarEx.Application.partitions_table(), {cluster, topic_name}) do
      [{{^cluster, ^topic_name}, {topic, partitions}}] ->
        {:ok, {topic, partitions}}

      [] ->
        GenServer.call(name(cluster), {:lookup, topic_name})
    end
  end

  def start_link(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)
    GenServer.start_link(__MODULE__, cluster_opts, name: name(cluster))
  end

  def init(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)
    brokers = Keyword.fetch!(cluster_opts, :brokers)
    admin_port = Keyword.fetch!(cluster_opts, :admin_port)

    {:ok,
     %State{
       cluster: cluster,
       cluster_opts: cluster_opts,
       brokers: brokers,
       admin_port: admin_port
     }}
  end

  def handle_call(
        {:lookup, topic_name},
        _from,
        %{cluster: cluster} = state
      ) do
    with [] <- :ets.lookup(PulsarEx.Application.partitions_table(), {cluster, topic_name}),
         {:ok, %Topic{} = topic} <- Topic.parse(topic_name),
         {:ok, partitions} <-
           Admin.lookup_topic_partitions(state.brokers, state.admin_port, topic) do
      :ets.insert(
        PulsarEx.Application.partitions_table(),
        {{cluster, topic_name}, {topic, partitions}}
      )

      if partitions > 0 do
        Process.send_after(
          self(),
          {:watch, topic_name, topic},
          @watch_interval + :rand.uniform(@watch_interval)
        )
      end

      {:reply, {:ok, {topic, partitions}}, state}
    else
      [{{^cluster, ^topic_name}, {topic, partitions}}] ->
        {:reply, {:ok, {topic, partitions}}, state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  def handle_info(
        {:watch, topic_name, topic},
        state
      ) do
    case Admin.lookup_topic_partitions(state.brokers, state.admin_port, topic) do
      {:ok, partitions} ->
        :ets.insert(
          PulsarEx.Application.partitions_table(),
          {{state.cluster, topic_name}, {topic, partitions}}
        )

      {:error, err} ->
        Logger.error(
          "Error watching topic partitions for #{topic_name}, on cluster #{state.cluster}, #{inspect(err)}"
        )
    end

    Process.send_after(
      self(),
      {:watch, topic_name, topic},
      @watch_interval + :rand.uniform(@watch_interval)
    )

    {:noreply, state}
  end

  defp name(cluster), do: String.to_atom("#{__MODULE__}.#{cluster}")
end
