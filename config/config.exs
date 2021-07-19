use Mix.Config

config :logger_json, :backend, metadata: :all

config :pulsar_ex, producer_module: PulsarEx

import_config "#{Mix.env()}.exs"
