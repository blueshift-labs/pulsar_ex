use Mix.Config

config :logger, :console,
  format: "$time [$level] $message $metadata\n",
  metadata: [:application, :module, :payload, :job]

config :pulsar_ex,
  brokers: ["localhost"],
  admin_port: 8080,
  socket_opts: [],
  timeout: 5_000,
  shutdown_timeout: 1000,
  num_connections: 5

config :pulsar_ex,
  producer_opts: [
    auto_start: true,
    batch_enabled: true,
    batch_size: 100,
    flush_interval: 1_000,
    properties: [client: "pulsar_ex"],
    refresh_interval: 10_000,
    num_producers: 1
  ],
  consumer_opts: [
    auto_start: false,
    properties: [client: "pulsar_ex"]
  ],
  producers: [
    [
      topic: "persistent://public/default/1.json",
      producer_access_mode: :shared,
      properties: [test: true],
      refresh_interval: 15_000,
      num_producers: 20,
      termination_timeout: 3_000
    ]
  ],
  consumers: [
    [
      topic: "persistent://public/default/test.json",
      subscription: "test",
      properties: [test: false],
      refresh_interval: 15_000,
      num_consumers: 3,
      module: PulsarEx.DefaultWorker
    ]
  ]

import_config "#{Mix.env()}.exs"
