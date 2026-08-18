import Config

config :logger, backends: []

config :pulsar_ex, :statsd, host: "localhost", port: 8125

config :pulsar_ex,
  cluster_name: "load",
  brokers: ["localhost"],
  port: 6650,
  admin_port: 8080,
  num_connections: 1,
  auto_setup: true,
  producer_opts: [compression: :lz4],
  tenants: ["pulsar_ex"],
  namespaces: ["pulsar_ex/Load"]
