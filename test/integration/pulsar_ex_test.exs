defmodule PulsarExTest do
  use ExUnit.Case
  use ExUnitProperties
  use Divo, services: [:pulsar]

  doctest PulsarEx

  @moduletag timeout: :infinity

  property "produce is always successful" do
    check all(
            message <- binary(),
            partition_key <- string(:ascii),
            ordering_key <- string(:ascii),
            event_time <- integer(),
            val1 <- string(:ascii),
            val2 <- string(:ascii)
          ) do
      assert {:messageId, _, _, _, _, _} =
               PulsarEx.produce("test.json", message,
                 partition_key: partition_key,
                 ordering_key: ordering_key,
                 event_time: event_time,
                 properties: [key1: val1, key2: val2]
               )
    end
  end

  test "producer consumer loop" do
    defmodule TestCallback do
      @behaviour PulsarEx.ConsumerCallback
      @compiled_pid self()
      alias PulsarEx.Pulserl.Structures.ConsumerMessage

      def handle_messages(msgs, _state) do
        msgs
        |> Enum.map(fn %ConsumerMessage{} = msg ->
          send(@compiled_pid, {:test_callback, msg})

          {msg, :ack}
        end)
      end
    end

    PulsarEx.start_consumers("test-loop.json", "test", TestCallback)

    message = "hello"
    partition_key = "123"
    ordering_key = "321"
    event_time = :os.system_time(:millisecond)

    assert {:messageId, _, _, _, _, _} =
             PulsarEx.produce("test-loop.json", message,
               partition_key: partition_key,
               ordering_key: ordering_key,
               event_time: event_time,
               properties: [test: true]
             )

    Process.sleep(2_000)

    assert_receive {:test_callback,
                    %PulsarEx.Pulserl.Structures.ConsumerMessage{
                      partition_key: ^partition_key,
                      ordering_key: ^ordering_key,
                      event_time: ^event_time,
                      properties: %{"test" => "true"},
                      payload: ^message
                    }}

    PulsarEx.stop_consumers("test-loop.json", "test")
  end

  test "delayed delivery" do
    defmodule TestCallback do
      @behaviour PulsarEx.ConsumerCallback
      @compiled_pid self()
      alias PulsarEx.Pulserl.Structures.ConsumerMessage

      def handle_messages(msgs, _state) do
        msgs
        |> Enum.map(fn %ConsumerMessage{} = msg ->
          send(@compiled_pid, {:test_callback, msg})
          send(@compiled_pid, {:receiver_ts, :os.system_time(:millisecond)})

          {msg, :ack}
        end)
      end
    end

    PulsarEx.start_consumers("test-delayed.json", "test", TestCallback)

    message = "hello"
    partition_key = "123"
    ordering_key = "321"
    event_time = :os.system_time(:millisecond)
    delay = 5_000

    assert {:messageId, _, _, _, _, _} =
             PulsarEx.produce("test-delayed.json", message,
               partition_key: partition_key,
               ordering_key: ordering_key,
               event_time: event_time,
               properties: [test: true],
               deliver_at_time: event_time + delay
             )

    assert_receive {:test_callback,
                    %PulsarEx.Pulserl.Structures.ConsumerMessage{
                      partition_key: ^partition_key,
                      ordering_key: ^ordering_key,
                      event_time: ^event_time,
                      properties: %{"test" => "true"},
                      payload: ^message
                    }},
                   10_000

    receive do
      {:receiver_ts, ts} ->
        assert_in_delta(ts - event_time, delay, 2_000)
    end

    PulsarEx.stop_consumers("test-delayed.json", "test")
  end
end
