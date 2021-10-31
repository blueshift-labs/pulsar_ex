use Mix.Config

config :logger, :console,
  format: "$time [$level] $message $metadata\n",
  metadata: [:application, :module, :payload, :job]

config :pulsar_ex,
  brokers: ["localhost"],
  admin_port: 8080,
  socket_opts: [],
  connection_timeout: 5_000,
  num_connections: 1

config :pulsar_ex,
  producer_opts: [
    auto_start: false,
    batch_enabled: false,
    batch_size: 100,
    flush_interval: 3000,
    properties: [client: "pulsar_ex"],
    refresh_interval: 1000,
    num_producers: 1
  ],
  consumer_opts: [
    auto_start: true,
    properties: [client: "pulsar_ex"]
  ],
  producers: [
    # [
    #   topic: "persistent://public/default/test.json",
    #   producer_access_mode: :shared,
    #   properties: [test: true]
    # ]
  ],
  consumers: [
    # [
    #   topic: "persistent://public/default/test.json",
    #   subscription: "test",
    #   module: PulsarEx.TestWorker,
    #   properties: [test: false],
    #   refresh_interval: 15_000,
    #   num_consumers: 30,
    #   module: PulsarEx.DefaultWorker
    # ]
  ]

import_config "#{Mix.env()}.exs"
