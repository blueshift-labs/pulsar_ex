defmodule PulsarEx.LoadEnvironmentTest do
  use ExUnit.Case, async: true

  test "the load environment is configured with its own cluster name" do
    assert Application.get_env(:pulsar_ex, :cluster_name) == "load"
  end
end
