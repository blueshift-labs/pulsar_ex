defmodule PulsarEx.RetryStrategy.ExponentialRetry do
  @behaviour PulsarEx.RetryStrategy

  alias PulsarEx.Backoff

  require Logger

  @max_attempts 5
  @backoff_type :rand_exp
  @backoff_min 1_000
  @backoff_max 10_000

  @client_errors [
    :message_size_too_large,
    :batch_empty,
    :batch_not_enabled,
    :batch_size_too_large,
    :message_delayed_in_batch
  ]

  @impl true
  def default_options() do
    [
      max_attempts: @max_attempts,
      backoff_type: @backoff_type,
      backoff_min: @backoff_min,
      backoff_max: @backoff_max
    ]
  end

  @impl true
  def new(options) do
    max_attempts = Keyword.get(options, :max_attempts, @max_attempts)

    options
    |> Enum.into(%{})
    |> Map.merge(%{attempts: 0, max_attempts: max_attempts, backoff: Backoff.new(options)})
  end

  @impl true
  def retry(err, _options, %{attempts: attempts, max_attempts: max_attempts} = state)
      when attempts >= max_attempts do
    Logger.error("Failed producing message, #{inspect(err)}", state)

    :telemetry.execute([:pulsar_ex, :produce, :failed], %{count: 1}, state)

    :error
  end

  def retry(err, options, %{attempts: attempts, backoff: backoff} = state)
      when err not in @client_errors do
    Logger.warning("Error producing message, #{inspect(err)}", state)

    :telemetry.execute([:pulsar_ex, :produce, :retry], %{count: 1}, state)

    {wait, backoff} = Backoff.backoff(backoff)
    Process.sleep(wait)
    {:retry, options, %{state | attempts: attempts + 1, backoff: backoff}}
  end

  def retry(err, _options, state) do
    Logger.error("Failed producing message, #{inspect(err)}", state)

    :telemetry.execute([:pulsar_ex, :produce, :failed], %{count: 1}, state)

    :error
  end
end
