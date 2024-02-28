defmodule PulsarEx.MixProject do
  use Mix.Project

  def project do
    [
      name: "PulsarEx",
      app: :pulsar_ex,
      version: "0.14.0",
      elixir: "~> 1.15.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      description: description(),
      deps: deps(),
      test_paths: test_paths(Mix.env()),
      aliases: aliases(),
      package: package(),
      source_url: "https://github.com/blueshift-labs/pulsar_ex"
    ]
  end

  defp description() do
    "A pulsar client implemented purely in Elixir"
  end

  defp aliases do
    [
      test: "test --color"
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {PulsarEx.Application, []}
    ]
  end

  defp test_paths(:test), do: ["test/unit"]
  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test"]

  defp elixirc_paths(:test), do: ["test/support", "lib"]
  defp elixirc_paths(:integration), do: ["test/support", "lib"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:telemetry, "~> 1.1"},
      {:telemetry_metrics, "~> 0.6", only: [:test, :integration]},
      {:telemetry_metrics_statsd, "~> 0.7", only: [:test, :integration]},
      {:telemetry_poller, "~> 1.0", only: [:test, :integration]},
      {:nimble_lz4, "~> 0.1.2", optional: true},
      {:hackney, "~> 1.18"},
      {:crc32cer, "0.1.10"},
      {:protobuf, "~> 0.11.0"},
      {:connection, "~> 1.1"},
      {:poolboy, "~> 1.5"},
      {:timex, "~> 3.7"},
      {:jason, "~> 1.2"}
    ]
  end

  defp package() do
    [
      files: ~w(lib .formatter.exs mix.exs README* LICENSE*),
      licenses: ~w(MIT),
      links: %{"GitHub" => "https://github.com/blueshift-labs/pulsar_ex"}
    ]
  end
end
