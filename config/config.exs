import Config

config :logger, level: :debug

config :logger, :console,
  format: "$time [$level] $message $metadata\n",
  metadata: [
    :application,
    :module,
    :cluster,
    :broker,
    :topic,
    :producer_id,
    :consumer_id
  ],
  compile_time_purge_matching: [
    [application: :tzdata]
  ]

import_config "#{Mix.env()}.exs"
