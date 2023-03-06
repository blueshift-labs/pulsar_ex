defmodule PulsarEx.ProducerTest do
  use ExUnit.Case

  alias PulsarEx.TestConnection

  describe "when batch is disabled" do
    @tag timeout: :infinity
    test "timeout sending single messages" do
      cluster_name = "unit"
      topic_name = "persistent://pulsar_ex/ProducerTest/single_message_timeout"

      assert {:error, :timeout} =
               PulsarEx.Cluster.produce(cluster_name, topic_name, "timeout", [timeout: 100],
                 batch_enabled: false
               )
    end

    @tag timeout: :infinity
    test "sends single messages in order" do
      cluster_name = "unit"
      topic_name = "persistent://pulsar_ex/ProducerTest/single_messages_in_order"

      n = 1000

      Enum.each(1..n, fn idx ->
        {:ok, _} =
          PulsarEx.Cluster.produce(cluster_name, topic_name, "instant-#{idx}", [],
            batch_enabled: false
          )
      end)

      messages = TestConnection.producer_messages(topic_name)

      assert Enum.to_list(1..n) ==
               Enum.map(messages, fn {seq_id,
                                      %{sequence_id: sequence_id, payload: "instant-" <> payload}} ->
                 assert seq_id == sequence_id
                 {idx, _} = Integer.parse(payload)
                 assert seq_id == idx
                 idx
               end)
    end
  end

  describe "when batch is enabled" do
    test "sends a batch of messages" do
      cluster_name = "unit"
      topic_name = "persistent://pulsar_ex/ProducerTest/one_batched_messages"

      n = 100
      payload = Enum.map(1..n, fn idx -> "#{idx}" end)

      PulsarEx.Cluster.produce(cluster_name, topic_name, payload, [],
        batch_enabled: true,
        batch_size: n + 1
      )

      assert [{1, messages}] = TestConnection.producer_messages(topic_name)

      assert Enum.to_list(1..n) ==
               Enum.map(messages, fn %{payload: payload} ->
                 {idx, _} = Integer.parse(payload)
                 idx
               end)
    end

    @tag timeout: :infinity
    test "sends messages in a batch" do
      cluster_name = "unit"
      topic_name = "persistent://pulsar_ex/ProducerTest/single_batched_messages"

      n = 10

      Task.async_stream(
        1..n,
        fn idx ->
          Process.sleep(idx * 100)

          PulsarEx.Cluster.produce(cluster_name, topic_name, "#{idx}", [],
            batch_enabled: true,
            batch_size: n + 1,
            flush_interval: 5000
          )
        end,
        max_concurrency: 16,
        timeout: 30_000
      )
      |> Enum.to_list()

      assert [{1, messages}] = TestConnection.producer_messages(topic_name)

      assert Enum.to_list(1..n) ==
               Enum.map(messages, fn %{sequence_id: sequence_id, payload: payload} ->
                 {idx, _} = Integer.parse(payload)
                 assert sequence_id == idx
                 idx
               end)
    end

    @tag timeout: :infinity
    test "sends messages in multiple batches" do
      cluster_name = "unit"
      topic_name = "persistent://pulsar_ex/ProducerTest/multiple_atched_messages"

      n = 10

      Task.async_stream(
        1..n,
        fn idx ->
          Process.sleep(idx * 100)

          PulsarEx.Cluster.produce(cluster_name, topic_name, "#{idx}", [],
            batch_enabled: true,
            batch_size: n - 2,
            flush_interval: 5000
          )
        end,
        max_concurrency: 16,
        timeout: 30_000
      )
      |> Enum.to_list()

      assert [{1, batch1}, {9, batch2}] = TestConnection.producer_messages(topic_name)

      assert Enum.to_list(1..(n - 2)) ==
               Enum.map(batch1, fn %{sequence_id: sequence_id, payload: payload} ->
                 {idx, _} = Integer.parse(payload)
                 assert sequence_id == idx
                 idx
               end)

      assert Enum.to_list((n - 1)..n) ==
               Enum.map(batch2, fn %{sequence_id: sequence_id, payload: payload} ->
                 {idx, _} = Integer.parse(payload)
                 assert sequence_id == idx
                 idx
               end)
    end

    @tag timeout: :infinity
    test "sends messages in multiple batches when delayed messages comes" do
      cluster_name = "unit"

      topic_name =
        "persistent://pulsar_ex/ProducerTest/multiple_atched_messages_with_delayed_message"

      n = 10

      task =
        Task.async(fn ->
          Process.sleep((n - 5) * 110)

          PulsarEx.Cluster.produce(cluster_name, topic_name, "delayed", [delay: 30000],
            batch_enabled: true,
            batch_size: n + 2,
            flush_interval: 5000
          )
        end)

      Task.async_stream(
        1..n,
        fn idx ->
          Process.sleep(idx * 100)

          PulsarEx.Cluster.produce(cluster_name, topic_name, "#{idx}", [],
            batch_enabled: true,
            batch_size: n + 2,
            flush_interval: 5000
          )
        end,
        max_concurrency: 16,
        timeout: 30_000
      )
      |> Enum.to_list()

      Task.await(task)

      assert [{1, batch1}, {6, message}, {7, batch2}] =
               TestConnection.producer_messages(topic_name)

      assert Enum.to_list(1..(n - 5)) ==
               Enum.map(batch1, fn %{sequence_id: sequence_id, payload: payload} ->
                 {idx, _} = Integer.parse(payload)
                 assert sequence_id == idx
                 idx
               end)

      assert Enum.to_list((n - 4)..n) ==
               Enum.map(batch2, fn %{sequence_id: sequence_id, payload: payload} ->
                 {idx, _} = Integer.parse(payload)
                 assert sequence_id == idx + 1
                 idx
               end)

      assert %{payload: "delayed"} = message
    end
  end

  describe "when recovery" do
    @tag timeout: :infinity
    test "sends all messsage without surprises" do
      cluster_name = "unit"
      topic_name = for _ <- 1..6, into: "", do: <<Enum.random('0123456789abcdef')>>
      topic_name = "persistent://pulsar_ex/ProducerTest/recovery-#{topic_name}"

      producer_opts = [
        batch_enabled: true,
        batch_size: 25,
        flush_interval: 3000,
        max_queue_size: 100_000
      ]

      n = 1000

      total =
        Task.async_stream(
          1..n,
          fn idx ->
            Process.sleep(:rand.uniform(100))

            batched? = [true, false] |> Enum.random()
            delay = [nil, 5000] |> Enum.random()

            cond do
              batched? ->
                m = :rand.uniform(20)
                payload = Enum.map(1..m, fn i -> "#{idx}.#{i}" end)

                assert {:ok, _} =
                         PulsarEx.Cluster.produce(
                           cluster_name,
                           topic_name,
                           payload,
                           [],
                           producer_opts
                         )

                m

              true ->
                assert {:ok, _} =
                         PulsarEx.Cluster.produce(
                           cluster_name,
                           topic_name,
                           "#{idx}",
                           [delay: delay],
                           producer_opts
                         )

                1
            end
          end,
          max_concurrency: 16,
          timeout: :infinity
        )
        |> Enum.map(fn {:ok, count} -> count end)
        |> Enum.sum()

      messages =
        TestConnection.producer_messages(topic_name)
        |> Enum.reduce(0, fn
          {_seq_id, msgs}, acc when is_list(msgs) -> acc + length(msgs)
          {_seq_id, _}, acc -> acc + 1
        end)

      assert total == messages
    end

    @tag timeout: :infinity
    test "sends all messsage when producer is closed" do
      cluster_name = "unit"
      topic_name = for _ <- 1..6, into: "", do: <<Enum.random('0123456789abcdef')>>
      topic_name = "persistent://pulsar_ex/ProducerTest/recovery-#{topic_name}"

      producer_opts = [
        batch_enabled: true,
        batch_size: 10,
        flush_interval: 1000,
        max_queue_size: 100_000
      ]

      task =
        Task.async(fn ->
          Process.sleep(2_000)

          PulsarEx.Cluster.produce(
            cluster_name,
            topic_name,
            "close",
            [delay: 1000],
            producer_opts
          )
        end)

      n = 1000

      total =
        Task.async_stream(
          1..n,
          fn idx ->
            Process.sleep(:rand.uniform(100))

            batched? = [true, false] |> Enum.random()
            delay = [nil, 5000] |> Enum.random()

            cond do
              batched? ->
                m = :rand.uniform(10)
                payload = Enum.map(1..m, fn i -> "#{idx}.#{i}" end)

                assert {:ok, _} =
                         PulsarEx.Cluster.produce(
                           cluster_name,
                           topic_name,
                           payload,
                           [],
                           producer_opts
                         )

                m

              true ->
                assert {:ok, _} =
                         PulsarEx.Cluster.produce(
                           cluster_name,
                           topic_name,
                           "#{idx}",
                           [delay: delay],
                           producer_opts
                         )

                1
            end
          end,
          max_concurrency: 16,
          timeout: :infinity
        )
        |> Enum.map(fn {:ok, count} -> count end)
        |> Enum.sum()

      Task.await(task)

      messages =
        TestConnection.producer_messages(topic_name)
        |> Enum.flat_map(fn
          {_seq_id, msgs} when is_list(msgs) ->
            Enum.map(msgs, fn %{payload: payload} -> payload end)

          {_seq_id, %{payload: payload}} ->
            [payload]
        end)

      assert total == messages |> Enum.uniq() |> Enum.count()
    end

    @tag timeout: :infinity
    test "sends all messsage when error at sending" do
      cluster_name = "unit"
      topic_name = for _ <- 1..6, into: "", do: <<Enum.random('0123456789abcdef')>>
      topic_name = "persistent://pulsar_ex/ProducerTest/recovery-#{topic_name}"

      producer_opts = [
        batch_enabled: true,
        batch_size: 10,
        flush_interval: 1000,
        max_queue_size: 100_000
      ]

      task =
        Task.async(fn ->
          Process.sleep(2_000)

          PulsarEx.Cluster.produce(
            cluster_name,
            topic_name,
            "error",
            [delay: 1000],
            producer_opts
          )
        end)

      n = 1000

      total =
        Task.async_stream(
          1..n,
          fn idx ->
            Process.sleep(:rand.uniform(100))

            batched? = [true, false] |> Enum.random()
            delay = [nil, 5000] |> Enum.random()

            cond do
              batched? ->
                m = :rand.uniform(10)
                payload = Enum.map(1..m, fn i -> "#{idx}.#{i}" end)

                assert {:ok, _} =
                         PulsarEx.Cluster.produce(
                           cluster_name,
                           topic_name,
                           payload,
                           [],
                           producer_opts
                         )

                m

              true ->
                assert {:ok, _} =
                         PulsarEx.Cluster.produce(
                           cluster_name,
                           topic_name,
                           "#{idx}",
                           [delay: delay],
                           producer_opts
                         )

                1
            end
          end,
          max_concurrency: 16,
          timeout: :infinity
        )
        |> Enum.map(fn {:ok, count} -> count end)
        |> Enum.sum()

      Task.await(task)

      messages =
        TestConnection.producer_messages(topic_name)
        |> Enum.flat_map(fn
          {_seq_id, msgs} when is_list(msgs) ->
            Enum.map(msgs, fn %{payload: payload} -> payload end)

          {_seq_id, %{payload: payload}} ->
            [payload]
        end)

      assert total == messages |> Enum.uniq() |> Enum.count()
    end
  end
end
