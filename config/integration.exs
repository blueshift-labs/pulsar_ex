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
  producer_opts: [compression: :lz4],
  tenants: ["pulsar_ex"],
  namespaces: ["pulsar_ex/IntegrationTest"],
  topics: [
    "persistent://pulsar_ex/IntegrationTest/SimpleTopicWorkerTest",
    "persistent://pulsar_ex/IntegrationTest/SimpleTopicWorkerTest.dead_letters",
    {"persistent://pulsar_ex/IntegrationTest/TestPartitionedTopicWorker", 15},
    "persistent://pulsar_ex/IntegrationTest/TestPartitionedTopicWorker.dead_letters",
    "persistent://pulsar_ex/IntegrationTest/RegexConsumerTest",
    "persistent://pulsar_ex/IntegrationTest/RegexWorkerTest",
    "persistent://pulsar_ex/IntegrationTest/RegexWorkerTest.dead_letters",
    "persistent://pulsar_ex/IntegrationTest/RegexConsumerDiscoveryTest",
    "persistent://pulsar_ex/IntegrationTest/RegexAnchorTest",
    "persistent://pulsar_ex/IntegrationTest/RegexAnchorTest.suffix",
    {"persistent://pulsar_ex/IntegrationTest/RegexPartitionFanoutTest", 5},
    "persistent://pulsar_ex/IntegrationTest/RegexNewTopicTestInitial",
    {"persistent://pulsar_ex/IntegrationTest/RegexPartitionExpansionTest", 3},
    "persistent://pulsar_ex/IntegrationTest/RegexOneTopicFailureTestFirst",
    "persistent://pulsar_ex/IntegrationTest/RegexOneTopicFailureTestLast",
    "persistent://pulsar_ex/IntegrationTest/RegexStopConsumerCrashTest",
    "persistent://pulsar_ex/IntegrationTest/RegexDeletionTestKeep",
    "persistent://pulsar_ex/IntegrationTest/RegexDeletionTestDrop",
    "persistent://pulsar_ex/IntegrationTest/RegexStopIndependentTest",
    "persistent://pulsar_ex/IntegrationTest/RegexDiscoveryTelemetryTestA",
    "persistent://pulsar_ex/IntegrationTest/RegexDiscoveryTelemetryTestB",
    {"persistent://pulsar_ex/IntegrationTest/PartitionedTopicStatsTest", 3}
  ]
