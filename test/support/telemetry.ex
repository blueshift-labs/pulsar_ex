defmodule PulsarEx.TestTelemetry do
  use Supervisor
  import Telemetry.Metrics

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init([]) do
    host = Application.fetch_env!(:pulsar_ex, :statsd) |> Keyword.fetch!(:host)
    port = Application.fetch_env!(:pulsar_ex, :statsd) |> Keyword.fetch!(:port)

    children = [
      {TelemetryMetricsStatsd,
       metrics: metrics(), formatter: :datadog, host: host, port: port, global_tags: global_tags()},
      {:telemetry_poller, measurements: periodic_measurements(), period: 1000}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def metrics do
    connection_metrics() ++ producer_metrics() ++ consumer_metrics()
  end

  defp connection_metrics() do
    [
      sum("pulsar_ex.connection.success.count"),
      sum("pulsar_ex.connection.error.count"),
      sum("pulsar_ex.connection.close_producer.request.success.count"),
      sum("pulsar_ex.connection.close_producer.request.error.count"),
      sum("pulsar_ex.connection.close_consumer.request.success.count"),
      sum("pulsar_ex.connection.close_consumer.request.error.count"),
      sum("pulsar_ex.connection.create_producer.request.success.count"),
      sum("pulsar_ex.connection.create_producer.request.error.count"),
      sum("pulsar_ex.connection.subscribe.request.success.count"),
      sum("pulsar_ex.connection.subscribe.request.error.count"),
      sum("pulsar_ex.connection.lookup_topic_partitions.request.success.count"),
      sum("pulsar_ex.connection.lookup_topic_partitions.request.error.count"),
      sum("pulsar_ex.connection.lookup_topic.request.success.count"),
      sum("pulsar_ex.connection.lookup_topic.request.error.count"),
      sum("pulsar_ex.connection.flow_permits.request.success.count"),
      sum("pulsar_ex.connection.flow_permits.request.error.count"),
      sum("pulsar_ex.connection.redeliver.request.success.count"),
      sum("pulsar_ex.connection.redeliver.request.error.count"),
      sum("pulsar_ex.connection.ack.request.success.count"),
      sum("pulsar_ex.connection.ack.request.error.count"),
      sum("pulsar_ex.connection.send.request.success.count"),
      sum("pulsar_ex.connection.send.request.error.count"),
      sum("pulsar_ex.connection.ping.request.success.count"),
      sum("pulsar_ex.connection.ping.request.error.count"),
      sum("pulsar_ex.connection.pong.request.success.count"),
      sum("pulsar_ex.connection.pong.request.error.count"),
      sum("pulsar_ex.connection.requests.timeout.count"),
      last_value("pulsar_ex.connection.requests.size"),
      sum("pulsar_ex.connection.exit.count"),
      sum("pulsar_ex.connection.ping.received.count"),
      sum("pulsar_ex.connection.pong.received.count"),
      sum("pulsar_ex.connection.close_producer.received.count"),
      sum("pulsar_ex.connection.close_consumer.received.count"),
      sum("pulsar_ex.connection.producer_ready.received.count"),
      summary("pulsar_ex.connection.producer_ready.received.duration"),
      sum("pulsar_ex.connection.producer_ready.missing.count"),
      sum("pulsar_ex.connection.producer_not_ready.received.count"),
      summary("pulsar_ex.connection.producer_not_ready.received.duration"),
      sum("pulsar_ex.connection.producer_not_ready.missing.count"),
      sum("pulsar_ex.connection.consumer_ready.received.count"),
      summary("pulsar_ex.connection.consumer_ready.received.duration"),
      sum("pulsar_ex.connection.close_producer.success.count"),
      sum("pulsar_ex.connection.close_consumer.success.count"),
      sum("pulsar_ex.connection.command_success.missing.count"),
      sum("pulsar_ex.connection.create_producer.error.count"),
      summary("pulsar_ex.connection.create_producer.error.duration"),
      sum("pulsar_ex.connection.subscribe.error.count"),
      summary("pulsar_ex.connection.subscribe.error.duration"),
      sum("pulsar_ex.connection.close_producer.error.count"),
      summary("pulsar_ex.connection.close_producer.error.duration"),
      sum("pulsar_ex.connection.close_consumer.error.count"),
      summary("pulsar_ex.connection.close_consumer.error.duration"),
      sum("pulsar_ex.connection.command_error.missing.count"),
      sum("pulsar_ex.connection.send_success.received.count"),
      summary("pulsar_ex.connection.send_success.received.duration"),
      sum("pulsar_ex.connection.send_success.missing.count"),
      sum("pulsar_ex.connection.send_error.received.count"),
      summary("pulsar_ex.connection.send_error.received.duration"),
      sum("pulsar_ex.connection.send_error.missing.count"),
      sum("pulsar_ex.connection.ack_success.received.count"),
      summary("pulsar_ex.connection.ack_success.received.duration"),
      sum("pulsar_ex.connection.ack_success.missing.count"),
      sum("pulsar_ex.connection.ack_error.received.count"),
      summary("pulsar_ex.connection.ack_error.received.duration"),
      sum("pulsar_ex.connection.ack_error.missing.count"),
      sum("pulsar_ex.connection.lookup_topic_partitions_success.received.count"),
      summary("pulsar_ex.connection.lookup_topic_partitions_success.received.duration"),
      sum("pulsar_ex.connection.lookup_topic_partitions_success.missing.count"),
      sum("pulsar_ex.connection.lookup_topic_partitions_error.received.count"),
      summary("pulsar_ex.connection.lookup_topic_partitions_error.received.duration"),
      sum("pulsar_ex.connection.lookup_topic_partitions_error.missing.count"),
      sum("pulsar_ex.connection.lookup_topic_connect.received.count"),
      summary("pulsar_ex.connection.lookup_topic_connect.received.duration"),
      sum("pulsar_ex.connection.lookup_topic_connect.missing.count"),
      sum("pulsar_ex.connection.lookup_topic_redirect.received.count"),
      summary("pulsar_ex.connection.lookup_topic_redirect.received.duration"),
      sum("pulsar_ex.connection.lookup_topic_redirect.missing.count"),
      sum("pulsar_ex.connection.lookup_topic_error.received.count"),
      summary("pulsar_ex.connection.lookup_topic_error.received.duration"),
      sum("pulsar_ex.connection.lookup_topic_error.missing.count"),
      sum("pulsar_ex.connection.unknown_command.received.count")
    ]
    |> Enum.map(fn metric ->
      metric
      |> Map.put(:tag_values, &pulsar_metadata/1)
      |> Map.put(:tags, [:cluster, :broker])
    end)
  end

  defp producer_metrics() do
    [
      sum("pulsar_ex.producer.max_attempts.count"),
      sum("pulsar_ex.producer.max_redirects.count"),
      sum("pulsar_ex.producer.lookup.success.count"),
      sum("pulsar_ex.producer.lookup.redirects.count"),
      sum("pulsar_ex.producer.lookup.error.count"),
      sum("pulsar_ex.producer.connect.success.count"),
      sum("pulsar_ex.producer.connect.error.count"),
      sum("pulsar_ex.producer.pending_sends.timeout.count"),
      last_value("pulsar_ex.producer.queue.size"),
      last_value("pulsar_ex.producer.pending_sends.size"),
      sum("pulsar_ex.producer.connection_down.count"),
      sum("pulsar_ex.producer.closed.count"),
      sum("pulsar_ex.producer.send.response.success.count"),
      summary("pulsar_ex.producer.send.response.success.duration"),
      sum("pulsar_ex.producer.send.response.missing.count"),
      sum("pulsar_ex.producer.send.response.error.count"),
      summary("pulsar_ex.producer.send.response.error.duration"),
      sum("pulsar_ex.producer.exit.count"),
      sum("pulsar_ex.producer.send.request.success.count"),
      sum("pulsar_ex.producer.send.request.error.count"),
      sum("pulsar_ex.producer.send.error.unknown.count"),
      sum("pulsar_ex.producer.send.error.timeout.count"),
      sum("pulsar_ex.producer.send.error.closed.count"),
      sum("pulsar_ex.producer.send.error.terminated.count"),
      sum("pulsar_ex.producer.send.error.exit.count"),
      sum("pulsar_ex.producer.send.error.connection_down.count"),
      sum("pulsar_ex.producer.send.error.too_many_redirects.count"),
      sum("pulsar_ex.producer.send.error.connection_not_ready.count")
    ]
    |> Enum.map(fn metric ->
      metric
      |> Map.put(:tag_values, &pulsar_metadata/1)
      |> Map.put(:tags, [:cluster, :broker, :topic])
    end)
  end

  defp consumer_metrics() do
    [
      sum("pulsar_ex.consumer.max_attempts.count"),
      sum("pulsar_ex.consumer.max_redirects.count"),
      sum("pulsar_ex.consumer.lookup.success.count"),
      sum("pulsar_ex.consumer.lookup.redirects.count"),
      sum("pulsar_ex.consumer.lookup.error.count"),
      sum("pulsar_ex.consumer.connect.success.count"),
      sum("pulsar_ex.consumer.connect.error.count"),
      last_value("pulsar_ex.consumer.queue.size"),
      last_value("pulsar_ex.consumer.pending_acks.size"),
      last_value("pulsar_ex.consumer.acks.size"),
      sum("pulsar_ex.consumer.ack_timeout.count"),
      sum("pulsar_ex.consumer.ack.success.count"),
      sum("pulsar_ex.consumer.ack.success.acks"),
      sum("pulsar_ex.consumer.ack.error.count"),
      sum("pulsar_ex.consumer.ack.error.acks"),
      sum("pulsar_ex.consumer.nacks.success.count"),
      sum("pulsar_ex.consumer.nacks.success.nacks"),
      sum("pulsar_ex.consumer.nacks.error.count"),
      sum("pulsar_ex.consumer.nacks.error.nacks"),
      sum("pulsar_ex.consumer.connection_down.count"),
      sum("pulsar_ex.consumer.closed.count"),
      sum("pulsar_ex.consumer.received.count"),
      sum("pulsar_ex.consumer.received.dead_letters.count"),
      sum("pulsar_ex.consumer.ack_response.success.count"),
      sum("pulsar_ex.consumer.ack_response.error.count"),
      sum("pulsar_ex.consumer.exit.count"),
      sum("pulsar_ex.consumer.flow_permits.success.count"),
      sum("pulsar_ex.consumer.flow_permits.success.permits"),
      sum("pulsar_ex.consumer.flow_permits.error.count"),
      sum("pulsar_ex.consumer.flow_permits.error.permits")
    ]
    |> Enum.map(fn metric ->
      metric
      |> Map.put(:tag_values, &pulsar_metadata/1)
      |> Map.put(:tags, [:cluster, :broker, :topic, :subscription, :subscription_type])
    end)
  end

  defp periodic_measurements do
    []
  end

  defp pulsar_metadata(tags) do
    broker = Map.get(tags, :broker) || Map.get(tags, :broker_url)

    tags
    |> Map.take([:cluster, :topic, :subscription, :subscription_type])
    |> Map.put(:broker, broker)
    |> Enum.reject(&match?({_, nil}, &1))
    |> Enum.reduce(%{}, fn {k, v}, acc -> Map.put(acc, k, to_string(v)) end)
  end

  defp global_tags() do
    Application.get_env(:telemetry, :global_tags, [])
  end
end
