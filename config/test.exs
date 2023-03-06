import Config

config :logger, backends: []

config :pulsar_ex, :statsd, host: "localhost", port: 8125

config :pulsar_ex,
  connection_module: PulsarEx.TestConnection,
  cluster_name: "unit",
  brokers: ["localhost"],
  port: 66500,
  admin_port: 80800,
  num_connections: 1
