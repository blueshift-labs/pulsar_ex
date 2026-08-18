defmodule PulsarEx.LoadAdminTest do
  use ExUnit.Case

  alias PulsarEx.{Admin, LoadAdmin}

  # config/test.exs sets admin_port: 80800 deliberately - it's out of TCP range
  # so that unit tests (which stub the broker connection via TestConnection)
  # never accidentally make real Admin HTTP calls. Real Admin calls in this repo
  # only happen under :integration/:load, where admin_port is 8080 (the
  # docker-compose "pulsar" service). We target that same real port directly
  # here instead of pulling admin_port from config.
  @brokers ["localhost"]
  @admin_port 8080

  test "delegates to PulsarEx.Admin and records call count/latency" do
    LoadAdmin.reset()

    expected = Admin.discover_clusters(@brokers, @admin_port)
    actual = LoadAdmin.discover_clusters(@brokers, @admin_port)

    assert actual == expected

    stats = LoadAdmin.stats()

    assert %{count: count, max_us: max_us} = stats[:discover_clusters]
    assert count >= 1
    assert max_us >= 0
  end
end
