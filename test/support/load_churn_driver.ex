defmodule PulsarEx.LoadChurnDriver do
  @moduledoc """
  Spreads topic creation, then deletion, over a duration at a steady rate -
  instead of provisioning everything in one batch like `LoadTopics.provision/4`
  does - so churn scenarios (Phase 6 Task 6.A.2) exercise discovery against
  topics arriving/leaving over time rather than all at once.

  Runs in two back-to-back phases, not fully interleaved: every `create_count`
  topic is created first (spread over its share of `duration_ms`), then
  `delete_count` of the *oldest* created topics are deleted (spread over the
  remaining share). This keeps scheduling simple and guarantees a delete never
  races a create for the same topic. A caller wanting true interleaving needs a
  different driver.

  The caller is expected to already have a regex consumer running against
  `prefix` before calling `run/1`, so created/deleted topics are actually
  observed by something.
  """

  alias PulsarEx.{LoadTopics, Admin}

  @doc """
  Required opts: `:prefix`, `:create_count`, `:duration_ms`.
  Optional: `:partitions` (default 1), `:delete_count` (default 0, capped at
  `create_count` - deleting more than was created is a no-op past that point),
  `:offset` (default 0), `:admin_module` (default `PulsarEx.Admin`).

  Returns `%{created: [topic_url, ...], deleted: [topic_url, ...]}` - `created`
  includes topics later deleted (callers need the full set to know what
  discovery should have seen at some point, not just what's live at the end).
  """
  def run(opts) do
    prefix = Keyword.fetch!(opts, :prefix)
    partitions = Keyword.get(opts, :partitions, 1)
    create_count = Keyword.fetch!(opts, :create_count)
    delete_count = Keyword.get(opts, :delete_count, 0)
    duration_ms = Keyword.fetch!(opts, :duration_ms)
    offset = Keyword.get(opts, :offset, 0)
    admin_module = Keyword.get(opts, :admin_module, Admin)

    total = create_count + delete_count
    create_duration_ms = if total > 0, do: div(duration_ms * create_count, total), else: 0
    delete_duration_ms = duration_ms - create_duration_ms

    created =
      create_phase(prefix, partitions, create_count, offset, create_duration_ms, admin_module)

    deleted = delete_phase(created, delete_count, delete_duration_ms, admin_module)

    %{created: created, deleted: deleted}
  end

  defp create_phase(_prefix, _partitions, 0, _offset, _duration_ms, _admin_module), do: []

  defp create_phase(prefix, partitions, create_count, offset, duration_ms, admin_module) do
    interval_ms = div(duration_ms, create_count)

    offset..(offset + create_count - 1)
    |> Enum.map(fn idx ->
      [topic] = LoadTopics.provision(prefix, 1, partitions, idx, admin_module)
      if interval_ms > 0, do: Process.sleep(interval_ms)
      topic
    end)
  end

  defp delete_phase(_created, 0, _duration_ms, _admin_module), do: []

  defp delete_phase(created, delete_count, duration_ms, admin_module) do
    to_delete = Enum.take(created, delete_count)
    interval_ms = if to_delete == [], do: 0, else: div(duration_ms, length(to_delete))

    brokers = Application.fetch_env!(:pulsar_ex, :brokers)
    admin_port = Application.fetch_env!(:pulsar_ex, :admin_port)

    Enum.map(to_delete, fn topic ->
      # Pulsar's delete-partitioned-topic endpoint 409s with "Topic has active
      # producers/subscriptions" unless forced - and a churn scenario's whole
      # point is deleting topics a regex consumer has already subscribed to,
      # so force is the realistic case here, not an edge case to avoid.
      :ok = admin_module.delete_partitioned_topic(brokers, admin_port, topic, true)
      if interval_ms > 0, do: Process.sleep(interval_ms)
      topic
    end)
  end
end
