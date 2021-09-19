import Config

config :logger, level: :info

config :pulsar_ex,
  divo: [
    {DivoPulsar, [port: 8080, version: "2.8.1"]}
  ],
  divo_wait: [dwell: 10_000, max_tries: 50]
