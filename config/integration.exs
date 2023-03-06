import Config

config :logger, backends: []

config :pulsar_ex, :statsd, host: "localhost", port: 8125

config :pulsar_ex,
  cluster_name: "integration",
  brokers: ["localhost"],
  port: 6650,
  admin_port: 8080,
  num_connections: 1,
  auto_setup: true,
  tenants: ["pulsar_ex"],
  namespaces: ["pulsar_ex/IntegrationTest"],
  topics: [
    "persistent://pulsar_ex/IntegrationTest/SimpleTopicWorkerTest",
    "persistent://pulsar_ex/IntegrationTest/SimpleTopicWorkerTest.dead_letters",
    {"persistent://pulsar_ex/IntegrationTest/TestPartitionedTopicWorker", 15},
    "persistent://pulsar_ex/IntegrationTest/TestPartitionedTopicWorker.dead_letters"
  ]
