import Config

config :pulsar_ex,
  shutdown_timeout: 5_000,
  brokers: ["localhost"],
  port: 6650,
  admin_port: 8080,
  socket_opts: [],
  connection_timeout: 5_000,
  num_connections: 1,
  producer_opts: [],
  consumer_opts: []
