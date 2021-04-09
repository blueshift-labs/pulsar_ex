use Mix.Config

config :pulserl,
  max_connections_per_broker: 3,
  service_url: "pulsar://localhost:6650",
  producer_opts: [
    producer_name: "test",
    batch_enable: true,
    properties: [test: true]
  ],
  consumer_opts: []

config :logger_json, :backend, metadata: :all
