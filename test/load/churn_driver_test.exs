defmodule PulsarEx.ChurnDriverTest do
  use ExUnit.Case

  alias PulsarEx.{Admin, LoadChurnDriver}

  # `LoadChurnDriver` (like `LoadTopics.provision/4` underneath it) reads
  # brokers/admin_port from application config rather than taking them as
  # parameters, so this only works under MIX_ENV=load's config (real broker on
  # localhost:8080) - it cannot run under test/unit's MIX_ENV=test config,
  # which deliberately points admin_port at an invalid port. Requires
  # `docker compose up -d pulsar` to be running, same as every other load test.
  @brokers Application.compile_env!(:pulsar_ex, :brokers)
  @admin_port Application.compile_env!(:pulsar_ex, :admin_port)

  test "creates topics then deletes the oldest of them, spread over duration_ms" do
    prefix = "ChurnDriverTest_#{System.unique_integer([:positive])}_"

    result =
      LoadChurnDriver.run(
        prefix: prefix,
        create_count: 3,
        delete_count: 1,
        duration_ms: 30
      )

    assert length(result.created) == 3
    assert length(result.deleted) == 1
    assert result.deleted == Enum.take(result.created, 1)

    [deleted_topic] = result.deleted
    still_alive_topics = result.created -- result.deleted

    assert {:error, _} = Admin.partitioned_topic_stats(@brokers, @admin_port, deleted_topic)

    for topic <- still_alive_topics do
      assert {:ok, _} = Admin.partitioned_topic_stats(@brokers, @admin_port, topic)
    end
  end

  test "delete_count of 0 leaves every created topic in place" do
    prefix = "ChurnDriverTest_#{System.unique_integer([:positive])}_"

    result = LoadChurnDriver.run(prefix: prefix, create_count: 2, duration_ms: 20)

    assert length(result.created) == 2
    assert result.deleted == []

    for topic <- result.created do
      assert {:ok, _} = Admin.partitioned_topic_stats(@brokers, @admin_port, topic)
    end
  end
end
