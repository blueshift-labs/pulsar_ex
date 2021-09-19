defmodule PulsarEx.Broker do
  alias __MODULE__

  @enforce_keys [:host, :port]
  defstruct [:host, :port]

  def parse(broker_url) when is_binary(broker_url) do
    case URI.parse(broker_url) do
      %URI{host: host, port: port} -> {:ok, %Broker{host: host, port: port}}
      _ -> {:error, :malformatted_broker_url}
    end
  end

  def to_name(%Broker{host: host, port: port}) do
    "#{host}:#{port}"
  end
end
