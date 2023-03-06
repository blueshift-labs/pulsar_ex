defmodule PulsarEx.ConsumerTest do
  use ExUnit.Case

  alias PulsarEx.TestConnection

  defmodule TestConsumer do
    use PulsarEx.Consumer

    @impl PulsarEx.ConsumerCallback
    def handle_messages(messages, _state) do
      Enum.map(messages, fn _ -> :ok end)
    end
  end

  defmodule TestRandomErrorConsumer do
    use PulsarEx.Consumer, max_redelivery_attempts: 3

    @impl PulsarEx.ConsumerCallback
    def handle_messages(messages, _state) do
      Enum.map(messages, fn
        %{redelivery_count: redelivery_count} when redelivery_count < 3 ->
          [:ok, {:error, :test}] |> Enum.random()

        _ ->
          :ok
      end)
    end
  end

  defmodule TestDeadLetteredConsumer do
    use PulsarEx.Consumer,
      max_redelivery_attempts: 3,
      dead_letter_topic:
        "persistent://pulsar_ex/ConsumerTest/consume_all_messages_with_dead_letters.dl"

    @dead_letter_topic "persistent://pulsar_ex/ConsumerTest/consume_all_messages_with_dead_letters.dl"

    @impl PulsarEx.ConsumerCallback
    def handle_messages(messages, _state) do
      Enum.map(messages, fn %{redelivery_count: redelivery_count} ->
        result =
          if :rand.uniform(10) < 5 do
            {:error, :test}
          else
            :ok
          end

        if result == {:error, :test} && redelivery_count == 3 do
          :ets.update_counter(:dead_letters, @dead_letter_topic, {2, 1}, {@dead_letter_topic, 0})
        end

        result
      end)
    end
  end

  setup_all do
    :ets.new(:dead_letters, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, []}
  end

  @tag timeout: :infinity
  test "consume all messsages" do
    cluster_name = "unit"
    topic_name = "persistent://pulsar_ex/ConsumerTest/consume_all_messages"
    subscription = "test"

    :ok =
      PulsarEx.Cluster.start_consumer(cluster_name, topic_name, subscription, TestConsumer,
        batch_size: 5,
        notify: {self(), :when_done}
      )

    receive do
      :done ->
        assert {[], []} = TestConnection.consumer_messages(topic_name)
    end
  end

  @tag timeout: :infinity
  test "consume all messsages, with random errors" do
    cluster_name = "unit"
    topic_name = "persistent://pulsar_ex/ConsumerTest/consume_all_messages_with_random_errors"
    subscription = "test"

    :ok =
      PulsarEx.Cluster.start_consumer(
        cluster_name,
        topic_name,
        subscription,
        TestRandomErrorConsumer,
        batch_size: 5,
        notify: {self(), :when_done}
      )

    receive do
      :done ->
        assert {[], []} = TestConnection.consumer_messages(topic_name)
    end
  end

  @tag timeout: :infinity
  test "consume all messsages, with random errors to dead letters topic" do
    cluster_name = "unit"
    topic_name = "persistent://pulsar_ex/ConsumerTest/consume_all_messages_with_dead_letters"

    dead_letter_topic =
      "persistent://pulsar_ex/ConsumerTest/consume_all_messages_with_dead_letters.dl"

    subscription = "test"

    :ok =
      PulsarEx.Cluster.start_consumer(
        cluster_name,
        topic_name,
        subscription,
        TestDeadLetteredConsumer,
        message_count: 1000,
        batch_size: 5,
        notify: {self(), :when_done}
      )

    receive do
      :done ->
        assert {[], []} = TestConnection.consumer_messages(topic_name)
        [{^dead_letter_topic, dead_letters}] = :ets.lookup(:dead_letters, dead_letter_topic)
        assert dead_letters == TestConnection.producer_messages(dead_letter_topic) |> Enum.count()
    end
  end
end
