import Config

config :logger, level: :info

config :pulserl,
  max_connections_per_broker: 1,
  service_url: "pulsar://localhost:6650",
  producer_opts: [
    batch_enable: true
  ]

config :pulsar_ex,
  divo: [
    {DivoPulsar, [port: 8080, version: "2.7.1"]}
  ],
  divo_wait: [dwell: 10_000, max_tries: 50]
