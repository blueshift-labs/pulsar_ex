defmodule PulsarEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :pulsar_ex,
      version: "0.1.2",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_paths: test_paths(Mix.env()),
      aliases: aliases(),
      package: package(),
      description: "Elixir client for Apache Pulsar, wrapped around erlang client pulserl"
    ]
  end

  defp aliases do
    [
      test: "test --no-start"
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {PulsarEx.Application, []}
    ]
  end

  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test/unit"]

  defp deps do
    [
      {:hackney, "~> 1.18"},
      {:crc32cer, "0.1.10"},
      {:protobuf, "~> 0.7.1"},
      {:connection, "~> 1.1"},
      {:poolboy, "~> 1.5"},
      {:telemetry, "~> 0.4"},
      {:timex, "~> 3.0"},
      {:gen_stage, "~> 1.1.2"},
      {:rollbax, "~> 0.11.0"},
      {:jason, "~> 1.2"}
    ]
  end

  defp package() do
    [
      files: ~w(lib include .formatter.exs mix.exs README* LICENSE*),
      licenses: ~w(MIT),
      links: %{"GitHub" => "https://github.com/blueshift-labs/pulsar_ex"}
    ]
  end
end
