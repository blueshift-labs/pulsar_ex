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
end

defimpl String.Chars, for: PulsarEx.Broker do
  def to_string(%PulsarEx.Broker{host: host, port: port}) do
    %URI{scheme: "pulsar", host: host, port: port} |> Kernel.to_string()
  end
end
