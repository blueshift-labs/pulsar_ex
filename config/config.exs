use Mix.Config

config :pulserl,
  max_connections_per_broker: 3,
  service_url: "pulsar://localhost:6650",
  producer_opts: [
    batch_enable: true
  ]

config :logger_json, :backend, metadata: :all
