use Mix.Config

config :logger_json, :backend, metadata: :all

import_config "#{Mix.env()}.exs"
