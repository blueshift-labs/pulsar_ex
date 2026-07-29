defmodule PulsarEx.TestRegexConsumer do
  use PulsarEx.Consumer

  @cluster "integration"
  @tenant "pulsar_ex"
  @namespace "IntegrationTest"
  @subscription "regex_consumer_test"

  def start(topic_regex, consumer_opts \\ []) do
    PulsarEx.Cluster.start_consumer(
      @cluster,
      @tenant,
      @namespace,
      topic_regex,
      @subscription,
      __MODULE__,
      Keyword.merge([initial_position: :earliest], consumer_opts)
    )
  end

  def stop(topic_regex) do
    PulsarEx.Cluster.stop_consumer(@cluster, @tenant, @namespace, topic_regex, @subscription)
  end

  def subscription(), do: @subscription

  def setup() do
    if :ets.whereis(:regex_consumer_test) == :undefined do
      :ets.new(:regex_consumer_test, [
        :named_table,
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])
    end
  end

  def destroy() do
    if :ets.whereis(:regex_consumer_test) != :undefined do
      :ets.delete(:regex_consumer_test)
    end
  end

  def received() do
    :ets.lookup(:regex_consumer_test, :received)
  end

  @impl true
  def handle_messages(messages, _state) do
    Enum.map(messages, fn _message ->
      :ets.update_counter(:regex_consumer_test, :received, {2, 1}, {:received, 0})

      :ok
    end)
  end
end
