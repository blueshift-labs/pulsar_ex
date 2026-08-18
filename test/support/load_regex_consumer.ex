defmodule PulsarEx.LoadRegexConsumer do
  use PulsarEx.Consumer

  alias PulsarEx.LoadTiming

  @cluster "load"
  @tenant "pulsar_ex"
  @namespace "Load"
  @subscription "load_test"
  @table :load_regex_consumer

  def start(topic_regex, consumer_opts \\ []) do
    PulsarEx.Cluster.start_consumer(
      @cluster,
      @tenant,
      @namespace,
      topic_regex,
      @subscription,
      __MODULE__,
      Keyword.merge([initial_position: :earliest, num_consumers: 1], consumer_opts)
    )
  end

  def stop(topic_regex) do
    PulsarEx.Cluster.stop_consumer(@cluster, @tenant, @namespace, topic_regex, @subscription)
  end

  def reset(topic_regex) do
    stop(topic_regex)
    destroy()
    setup()
  end

  def cluster(), do: @cluster
  def tenant(), do: @tenant
  def namespace(), do: @namespace
  def subscription(), do: @subscription

  def wait_until_active(desired_count, timeout_ms) do
    LoadTiming.wait_until_active(
      @cluster,
      @tenant,
      @namespace,
      @subscription,
      desired_count,
      timeout_ms
    )
  end

  def wait_until_ready(desired_count, timeout_ms) do
    LoadTiming.wait_until_ready(
      @cluster,
      @tenant,
      @namespace,
      @subscription,
      desired_count,
      timeout_ms
    )
  end

  def setup() do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [
        :named_table,
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])
    end
  end

  def destroy() do
    if :ets.whereis(@table) != :undefined do
      :ets.delete(@table)
    end
  end

  def received() do
    case :ets.lookup(@table, :received) do
      [{:received, count}] -> count
      [] -> 0
    end
  end

  @impl true
  def handle_messages(messages, _state) do
    Enum.map(messages, fn _message ->
      :ets.update_counter(@table, :received, {2, 1}, {:received, 0})

      :ok
    end)
  end
end
