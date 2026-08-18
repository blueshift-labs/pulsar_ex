defmodule PulsarEx.LoadTelemetryCounterTest do
  use ExUnit.Case

  alias PulsarEx.LoadTelemetryCounter

  test "tallies event counts and sums duration for discovery events" do
    handle = LoadTelemetryCounter.start()

    for _ <- 1..3, do: :telemetry.execute([:pulsar_ex, :consumer, :received], %{count: 1}, %{})
    :telemetry.execute([:pulsar_ex, :consumer, :received, :dead_letters], %{count: 1}, %{})

    :telemetry.execute(
      [:pulsar_ex, :consumer_manager, :discovery, :complete],
      %{duration: 10_000},
      %{}
    )

    :telemetry.execute(
      [:pulsar_ex, :consumer_manager, :discovery, :complete],
      %{duration: 5_000},
      %{}
    )

    :telemetry.execute(
      [:pulsar_ex, :consumer_manager, :discovery, :error],
      %{duration: 2_500},
      %{}
    )

    :telemetry.execute([:pulsar_ex, :consumer, :lookup, :success], %{}, %{})
    :telemetry.execute([:pulsar_ex, :consumer, :ack, :success], %{}, %{})
    :telemetry.execute([:pulsar_ex, :consumer, :nacks, :error], %{}, %{})
    :telemetry.execute([:pulsar_ex, :consumer, :max_attempts], %{}, %{})

    tallies = LoadTelemetryCounter.stop(handle)

    assert tallies[[:pulsar_ex, :consumer, :received]] == %{count: 3}
    assert tallies[[:pulsar_ex, :consumer, :received, :dead_letters]] == %{count: 1}

    assert tallies[[:pulsar_ex, :consumer_manager, :discovery, :complete]] == %{
             count: 2,
             duration_sum: 15_000
           }

    assert tallies[[:pulsar_ex, :consumer_manager, :discovery, :error]] == %{
             count: 1,
             duration_sum: 2_500
           }

    assert tallies[[:pulsar_ex, :consumer, :lookup, :success]] == %{count: 1}
    assert tallies[[:pulsar_ex, :consumer, :ack, :success]] == %{count: 1}
    assert tallies[[:pulsar_ex, :consumer, :nacks, :error]] == %{count: 1}
    assert tallies[[:pulsar_ex, :consumer, :max_attempts]] == %{count: 1}

    assert LoadTelemetryCounter.json_safe(tallies)["pulsar_ex.consumer.received"] == %{count: 3}
  end

  test "start/stop cycles never leak or collide handlers" do
    handle1 = LoadTelemetryCounter.start()

    assert attached_handler_count() == 1

    _tallies1 = LoadTelemetryCounter.stop(handle1)

    assert attached_handler_count() == 0

    # A second cycle must not collide with (or see stale counts from) the first.
    handle2 = LoadTelemetryCounter.start()

    :telemetry.execute([:pulsar_ex, :consumer, :received], %{count: 1}, %{})

    tallies2 = LoadTelemetryCounter.stop(handle2)

    assert tallies2[[:pulsar_ex, :consumer, :received]] == %{count: 1}
    assert attached_handler_count() == 0
  end

  test "two concurrent start/stop cycles do not interfere with each other's tallies" do
    handle_a = LoadTelemetryCounter.start()
    handle_b = LoadTelemetryCounter.start()

    assert attached_handler_count() == 2

    :telemetry.execute([:pulsar_ex, :consumer, :received], %{count: 1}, %{})
    :telemetry.execute([:pulsar_ex, :consumer, :received], %{count: 1}, %{})

    tallies_a = LoadTelemetryCounter.stop(handle_a)
    tallies_b = LoadTelemetryCounter.stop(handle_b)

    assert tallies_a[[:pulsar_ex, :consumer, :received]] == %{count: 2}
    assert tallies_b[[:pulsar_ex, :consumer, :received]] == %{count: 2}
    assert attached_handler_count() == 0
  end

  # `:telemetry.list_handlers/1` matches by event-name *prefix*, not exact
  # equality - `[:pulsar_ex, :consumer, :received]` is itself a prefix of
  # `[:pulsar_ex, :consumer, :received, :dead_letters]` (also in
  # LoadTelemetryCounter's attached event list), so querying by that event
  # name alone double-counts each attached session. Count distinct handler
  # ids instead - attach_many registers the same id once per event, so this
  # is exactly "how many LoadTelemetryCounter sessions are attached right now".
  defp attached_handler_count() do
    :telemetry.list_handlers([:pulsar_ex])
    |> Enum.map(& &1.id)
    |> Enum.uniq()
    |> length()
  end
end
