defmodule PulsarEx.LoadResourceSamplerTest do
  use ExUnit.Case

  alias PulsarEx.{ConsumerIDRegistry, ConsumerRegistry, LoadResourceSampler}

  test "returns timestamped samples with all fields, and picks up fake consumer/supervisor mailbox lengths via Registry, not per-consumer calls" do
    test_pid = self()

    # Fakes registered into ConsumerIDRegistry: stand in for individual
    # consumer GenServer processes (the ones that actually receive Pulsar
    # messages - see PulsarEx.active_consumers/4).
    consumer_pids =
      for i <- 1..2 do
        {:ok, pid} =
          Task.start(fn ->
            Registry.register(ConsumerIDRegistry, {"faketest", "fake-consumer-#{i}"}, nil)
            send(test_pid, {:consumer_registered, i})

            receive do
              :stop -> :ok
            end
          end)

        assert_receive {:consumer_registered, ^i}
        pid
      end

    # Fake registered into ConsumerRegistry: stands in for the per-topic
    # supervisor pid, per the task doc's suggested fake-registration shape.
    {:ok, supervisor_pid} =
      Task.start(fn ->
        Registry.register(ConsumerRegistry, {"faketest", "fake-topic-1", "fake-sub"}, nil)
        send(test_pid, :supervisor_registered)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :supervisor_registered

    {:ok, sampler} = LoadResourceSampler.start(10)

    Process.sleep(150)

    samples = LoadResourceSampler.stop(sampler)

    assert length(samples) >= 1

    Enum.each(samples, fn sample ->
      assert Map.has_key?(sample, :timestamp)
      assert is_integer(sample.timestamp)

      assert is_integer(sample.process_count)
      assert sample.process_count > 0

      assert is_list(sample.memory)
      assert Keyword.has_key?(sample.memory, :total)

      assert is_list(sample.scheduler_utilization)
      assert length(sample.scheduler_utilization) > 0

      Enum.each(sample.scheduler_utilization, fn entry ->
        assert is_integer(entry.scheduler_id)
        assert is_float(entry.utilization)
        assert entry.utilization >= 0.0
      end)

      assert is_integer(sample.rss_kb)
      assert sample.rss_kb > 0

      assert is_list(sample.mailbox_lengths)
      assert is_list(sample.supervisor_mailbox_lengths)
    end)

    # At least one sample must have observed the fake pids via the registry
    # scan - proving the sampler reads Registry.select/Process.info directly
    # instead of making a synchronous call into each consumer (a per-consumer
    # GenServer.call would hang, since these fakes never respond to calls).
    observed_consumer_pids =
      samples
      |> Enum.flat_map(& &1.mailbox_lengths)
      |> Enum.map(fn {pid, _len} -> pid end)
      |> MapSet.new()

    observed_supervisor_pids =
      samples
      |> Enum.flat_map(& &1.supervisor_mailbox_lengths)
      |> Enum.map(fn {pid, _len} -> pid end)
      |> MapSet.new()

    assert Enum.all?(consumer_pids, &MapSet.member?(observed_consumer_pids, &1))
    assert MapSet.member?(observed_supervisor_pids, supervisor_pid)

    [first | _] = LoadResourceSampler.json_safe(samples, hd(samples).timestamp)
    assert first.timestamp == 0
    assert is_map(first.memory)
    assert Enum.all?(first.mailbox_lengths, &is_integer/1)

    for pid <- [supervisor_pid | consumer_pids], do: send(pid, :stop)
  end
end
