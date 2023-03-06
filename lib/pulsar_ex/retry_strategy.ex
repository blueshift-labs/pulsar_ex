defmodule PulsarEx.RetryStrategy do
  @moduledoc """
  Adopted from Xandra.RetryStrategy
    https://github.com/lexhide/xandra/blob/main/lib/xandra/retry_strategy.ex
  then modified by Blueshift at:
    https://raw.githubusercontent.com/blueshift-labs/xandra/main/lib/xandra/retry_strategy.ex
  """

  @type state :: term

  @callback default_options() :: keyword

  @callback new(options :: keyword) :: state

  @callback retry(error :: term, options :: keyword, state) ::
              :error | {:retry, new_options :: keyword, new_state :: state}

  @doc false
  @spec run_with_retrying(keyword, (() -> result)) :: result when result: var
  def run_with_retrying(options, fun) do
    case Keyword.pop(options, :retry_strategy) do
      {nil, _options} -> fun.()
      {retry_strategy, options} -> run_with_retrying(options, retry_strategy, fun)
    end
  end

  defp run_with_retrying(options, retry_strategy, fun) do
    with {:error, reason} <- fun.() do
      {retry_state, options} =
        Keyword.pop_lazy(options, :retrying_state, fn ->
          retry_strategy.new(options)
        end)

      case retry_strategy.retry(reason, options, retry_state) do
        :error ->
          {:error, reason}

        {:retry, new_options, new_retry_state} ->
          new_options = Keyword.put(new_options, :retrying_state, new_retry_state)
          run_with_retrying(new_options, retry_strategy, fun)

        other ->
          raise ArgumentError,
                "invalid return value #{inspect(other)} from " <>
                  "retry strategy #{inspect(retry_strategy)} " <>
                  "with state #{inspect(retry_state)}"
      end
    end
  end
end
