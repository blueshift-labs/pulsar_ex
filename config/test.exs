import Config

config :logger, level: :info

config :pulserl,
  max_connections_per_broker: 1,
  service_url: "pulsar://localhost:6650"
