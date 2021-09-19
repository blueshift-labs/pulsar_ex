defmodule PulsarEx.Topic do
  alias __MODULE__

  @enforce_keys [:persistent, :tenant, :namespace, :topic]
  defstruct [:persistent, :tenant, :namespace, :topic, :partition]

  def parse(topic_name) when is_binary(topic_name) do
    with %URI{scheme: persistent, host: tenant, path: "/" <> path}
         when not is_nil(persistent) and not is_nil(tenant) and not is_nil(path) <-
           URI.parse(topic_name),
         [namespace, topic_name] <- String.split(path, "/", parts: 2) do
      with [topic_name, partition] <- String.split(topic_name, "-partition-", parts: 2),
           {partition, ""} when is_integer(partition) <- Integer.parse(partition) do
        {:ok,
         %Topic{
           persistent: persistent == "persistent",
           tenant: tenant,
           namespace: namespace,
           topic: topic_name,
           partition: partition
         }}
      else
        :error ->
          {:error, :malformatted_topic}

        {_, r} when r != "" ->
          {:error, :malformatted_topic}

        _ ->
          {:ok,
           %Topic{
             persistent: persistent == "persistent",
             tenant: tenant,
             namespace: namespace,
             topic: topic_name,
             partition: nil
           }}
      end
    else
      _ -> {:error, :malformatted_topic}
    end
  end

  def to_name(%Topic{} = topic), do: topic |> to_uri() |> URI.to_string()

  def to_logical_name(%Topic{} = topic),
    do: to_name(%{topic | partition: nil})

  def to_uri(%Topic{persistent: true, partition: nil} = topic) do
    %URI{
      scheme: "persistent",
      host: topic.tenant,
      path: "/#{topic.namespace}/#{topic.topic}"
    }
  end

  def to_uri(%Topic{persistent: true} = topic) do
    %URI{
      scheme: "persistent",
      host: topic.tenant,
      path: "/#{topic.namespace}/#{topic.topic}-partition-#{topic.partition}"
    }
  end

  def to_uri(%Topic{persistent: false, partition: nil} = topic) do
    %URI{
      scheme: "non-persistent",
      host: topic.tenant,
      path: "/#{topic.namespace}/#{topic.topic}"
    }
  end

  def to_uri(%Topic{persistent: false} = topic) do
    %URI{
      scheme: "non-persistent",
      host: topic.tenant,
      path: "/#{topic.namespace}/#{topic.topic}-partition-#{topic.partition}"
    }
  end

  def to_query(%Topic{persistent: true, partition: nil} = topic) do
    "persistent/#{topic.tenant}/#{topic.namespace}/#{topic.topic}"
  end

  def to_query(%Topic{persistent: true} = topic) do
    "persistent/#{topic.tenant}/#{topic.namespace}/#{topic.topic}-partition-#{topic.partition}"
  end

  def to_query(%Topic{persistent: false, partition: nil} = topic) do
    "non-persistent/#{topic.tenant}/#{topic.namespace}/#{topic.topic}"
  end

  def to_query(%Topic{persistent: false} = topic) do
    "non-persistent/#{topic.tenant}/#{topic.namespace}/#{topic.topic}-partition-#{topic.partition}"
  end
end
