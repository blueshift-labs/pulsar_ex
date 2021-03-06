defmodule PulsarEx.Admin do
  alias PulsarEx.{Broker, Topic}

  def health_check(%Broker{host: host}, admin_port) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/brokers/health"
    }

    with {:ok, 200, _, client_ref} <-
           :hackney.get(URI.to_string(url), [], "", []),
         {:ok, "ok"} <- :hackney.body(client_ref) do
      :ok
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      {:ok, body} ->
        {:error, body}

      err ->
        err
    end
  end

  def lookup_topic_partitions(hosts, admin_port, %Topic{} = topic)
      when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case lookup_topic_partitions(host, admin_port, topic) do
        {:ok, partitions} -> {:halt, {:ok, partitions}}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end

  def lookup_topic_partitions(host, admin_port, %Topic{} = topic)
      when is_binary(host) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/#{Topic.to_query(topic)}/partitions",
      query: "checkAllowAutoCreation=true"
    }

    with {:ok, 200, _, client_ref} <-
           :hackney.get(URI.to_string(url), [], "", follow_redirect: true),
         {:ok, body} <- :hackney.body(client_ref),
         {:ok, %{"partitions" => partitions}} <- Jason.decode(body) do
      {:ok, partitions}
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end

  def lookup_topic(hosts, admin_port, %Topic{} = topic) when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case lookup_topic(host, admin_port, topic) do
        {:ok, broker} -> {:halt, {:ok, broker}}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end

  def lookup_topic(host, admin_port, %Topic{} = topic) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/lookup/v2/topic/#{Topic.to_query(topic)}"
    }

    with {:ok, 200, _, client_ref} <-
           :hackney.get(URI.to_string(url), [], "", follow_redirect: true),
         {:ok, body} <- :hackney.body(client_ref),
         {:ok, %{"brokerUrl" => broker_url}} <- Jason.decode(body),
         {:ok, broker} <- Broker.parse(broker_url) do
      {:ok, broker}
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end

  defp lookup_partitioned_topics(hosts, admin_port, tenant, namespace) when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case lookup_partitioned_topics(host, admin_port, tenant, namespace) do
        {:ok, topics} -> {:halt, {:ok, topics}}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end

  defp lookup_partitioned_topics(host, admin_port, tenant, namespace) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/persistent/#{tenant}/#{namespace}/partitioned"
    }

    with {:ok, 200, _, client_ref} <-
           :hackney.get(URI.to_string(url), [], "", follow_redirect: true),
         {:ok, body} <- :hackney.body(client_ref),
         {:ok, topics} <- Jason.decode(body) do
      {:ok, topics}
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end

  defp lookup_topics(hosts, admin_port, tenant, namespace) when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case lookup_topics(host, admin_port, tenant, namespace) do
        {:ok, topics} -> {:halt, {:ok, topics}}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end

  defp lookup_topics(host, admin_port, tenant, namespace) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/persistent/#{tenant}/#{namespace}"
    }

    with {:ok, 200, _, client_ref} <-
           :hackney.get(URI.to_string(url), [], "", follow_redirect: true),
         {:ok, body} <- :hackney.body(client_ref),
         {:ok, topics} <- Jason.decode(body) do
      {:ok, Enum.reject(topics, &String.contains?(&1, "-partition-"))}
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end

  def discover_topics(hosts, admin_port, tenant, namespace, regex) do
    with {:ok, partitioned_topics} <-
           lookup_partitioned_topics(hosts, admin_port, tenant, namespace),
         {:ok, topics} <- lookup_topics(hosts, admin_port, tenant, namespace) do
      topics = Enum.filter(partitioned_topics ++ topics, &Regex.match?(regex, &1))
      {:ok, topics}
    else
      {:error, err} -> {:error, err}
    end
  end
end
