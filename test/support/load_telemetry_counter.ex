defmodule PulsarEx.LoadTelemetryCounter do
  @moduledoc """
  Load-scoped telemetry tally for Phase 4 scenarios.

  Attaches a single `:telemetry.attach_many/4` handler (unique handler id per
  `start/0` call) covering the Phase 2 discovery events and the existing
  per-consumer events (lookup, connect, ack/nack, received, max_attempts).
  Each event increments an O(1) ETS counter keyed by event name; the two
  discovery events additionally accumulate their `duration` measurement.

  Aggregated by event name only (never by topic/partition/consumer id), per
  the cardinality rule carried over from Phase 2.

  Usage:

      handle = PulsarEx.LoadTelemetryCounter.start()
      # ... run scenario ...
      tallies = PulsarEx.LoadTelemetryCounter.stop(handle)
      # => %{[:pulsar_ex, :consumer, :received] => %{count: 42}, ...}
  """

  @events [
    [:pulsar_ex, :consumer_manager, :discovery, :complete],
    [:pulsar_ex, :consumer_manager, :discovery, :error],
    [:pulsar_ex, :consumer, :max_attempts],
    [:pulsar_ex, :consumer, :connection_down],
    [:pulsar_ex, :consumer, :lookup, :success],
    [:pulsar_ex, :consumer, :lookup, :redirects],
    [:pulsar_ex, :consumer, :lookup, :error],
    [:pulsar_ex, :consumer, :connect, :success],
    [:pulsar_ex, :consumer, :connect, :error],
    [:pulsar_ex, :consumer, :ack, :success],
    [:pulsar_ex, :consumer, :ack, :error],
    [:pulsar_ex, :consumer, :nacks, :success],
    [:pulsar_ex, :consumer, :nacks, :error],
    [:pulsar_ex, :consumer, :received],
    [:pulsar_ex, :consumer, :received, :dead_letters]
  ]

  @duration_events [
    [:pulsar_ex, :consumer_manager, :discovery, :complete],
    [:pulsar_ex, :consumer_manager, :discovery, :error]
  ]

  defstruct [:handler_id, :table]

  @doc """
  Attaches the tally handler and creates a fresh (unnamed) ETS table for
  this scenario run. Returns a handle to pass to `stop/1`.
  """
  def start() do
    table =
      :ets.new(:load_telemetry_counter, [
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])

    handler_id = {__MODULE__, make_ref()}

    :ok = :telemetry.attach_many(handler_id, @events, &__MODULE__.handle_event/4, table)

    %__MODULE__{handler_id: handler_id, table: table}
  end

  @doc """
  Detaches the handler and returns the final tallies bucketed by event name,
  e.g. `%{[:pulsar_ex, :consumer, :received] => %{count: 42}}`.
  """
  def stop(%__MODULE__{handler_id: handler_id, table: table}) do
    :ok = :telemetry.detach(handler_id)

    tallies =
      table
      |> :ets.tab2list()
      |> Enum.reduce(%{}, fn {{event_name, kind}, value}, acc ->
        Map.update(acc, event_name, %{kind => value}, &Map.put(&1, kind, value))
      end)

    :ets.delete(table)

    tallies
  end

  def json_safe(tallies) do
    Map.new(tallies, fn {event_name, value} -> {Enum.join(event_name, "."), value} end)
  end

  @doc false
  def handle_event(event_name, measurements, _metadata, table) do
    :ets.update_counter(table, {event_name, :count}, {2, 1}, {{event_name, :count}, 0})

    if event_name in @duration_events do
      case measurements do
        %{duration: duration} ->
          :ets.update_counter(
            table,
            {event_name, :duration_sum},
            {2, duration},
            {{event_name, :duration_sum}, 0}
          )

        _ ->
          :ok
      end
    end

    :ok
  end
end
