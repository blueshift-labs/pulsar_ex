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

  defp lookup_tenants(hosts, admin_port) when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case lookup_tenants(host, admin_port) do
        {:ok, tenants} -> {:halt, {:ok, tenants}}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end

  defp lookup_tenants(host, admin_port) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/tenants"
    }

    with {:ok, 200, _, client_ref} <-
           :hackney.get(URI.to_string(url), [], "", follow_redirect: true),
         {:ok, body} <- :hackney.body(client_ref),
         {:ok, tenants} <- Jason.decode(body) do
      {:ok, tenants}
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end

  defp lookup_namespaces(hosts, admin_port, tenant) when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case lookup_namespaces(host, admin_port, tenant) do
        {:ok, namespaces} -> {:halt, {:ok, namespaces}}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end

  defp lookup_namespaces(host, admin_port, tenant) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/namespaces/#{tenant}"
    }

    with {:ok, 200, _, client_ref} <-
           :hackney.get(URI.to_string(url), [], "", follow_redirect: true),
         {:ok, body} <- :hackney.body(client_ref),
         {:ok, namespaces} <- Jason.decode(body) do
      {:ok, namespaces}
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end

  defp discover_tenants(hosts, admin_port, %Regex{} = tenant_regex) do
    with {:ok, tenants} <- lookup_tenants(hosts, admin_port) do
      tenants = Enum.filter(tenants, &Regex.match?(tenant_regex, &1))
      {:ok, tenants}
    end
  end

  defp discover_tenants(hosts, admin_port, tenant_name) do
    with {:ok, tenants} <- lookup_tenants(hosts, admin_port) do
      tenants = Enum.filter(tenants, &(tenant_name == &1))
      {:ok, tenants}
    end
  end

  defp discover_namespaces(hosts, admin_port, tenant, %Regex{} = namespace_regex) do
    with {:ok, tenants} <- discover_tenants(hosts, admin_port, tenant) do
      Enum.reduce_while(tenants, {:ok, []}, fn tenant, {:ok, acc} ->
        case lookup_namespaces(hosts, admin_port, tenant) do
          {:ok, namespaces} ->
            namespaces =
              namespaces
              |> Enum.map(&String.split(&1, "/", parts: 2))
              |> Enum.filter(fn [_, namespace] ->
                Regex.match?(namespace_regex, namespace)
              end)
              |> Enum.map(fn [tenant, namespace] ->
                {tenant, namespace}
              end)

            {:cont, {:ok, acc ++ namespaces}}

          {:error, err} ->
            {:halt, {:error, err}}
        end
      end)
    end
  end

  defp discover_namespaces(hosts, admin_port, tenant, namespace_name) do
    with {:ok, tenants} <- discover_tenants(hosts, admin_port, tenant) do
      Enum.reduce_while(tenants, {:ok, []}, fn tenant, {:ok, acc} ->
        case lookup_namespaces(hosts, admin_port, tenant) do
          {:ok, namespaces} ->
            namespaces =
              namespaces
              |> Enum.map(&String.split(&1, "/", parts: 2))
              |> Enum.filter(fn [_, namespace] ->
                namespace_name == namespace
              end)
              |> Enum.map(fn [tenant, namespace] ->
                {tenant, namespace}
              end)

            {:cont, {:ok, acc ++ namespaces}}

          {:error, err} ->
            {:halt, {:error, err}}
        end
      end)
    end
  end

  def discover_topics(hosts, admin_port, tenant, namespace, %Regex{} = topic_regex) do
    with {:ok, namespaces} <- discover_namespaces(hosts, admin_port, tenant, namespace) do
      Enum.reduce_while(namespaces, {:ok, []}, fn {tenant, namespace}, {:ok, acc} ->
        with {:ok, partitioned_topics} <-
               lookup_partitioned_topics(hosts, admin_port, tenant, namespace),
             {:ok, topics} <- lookup_topics(hosts, admin_port, tenant, namespace) do
          Enum.reduce_while(partitioned_topics ++ topics, {:ok, []}, fn topic, {:ok, acc} ->
            case Topic.parse(topic) do
              {:ok, %Topic{} = topic} ->
                if Regex.match?(topic_regex, topic.topic) do
                  {:cont, {:ok, [Topic.to_name(topic) | acc]}}
                else
                  {:cont, {:ok, acc}}
                end

              {:error, err} ->
                {:halt, {:error, err}}
            end
          end)
        end
        |> case do
          {:ok, topics} ->
            {:cont, {:ok, acc ++ topics}}

          {:error, err} ->
            {:halt, {:error, err}}
        end
      end)
    end
  end

  def discover_topics(hosts, admin_port, tenant, namespace, topic_name) do
    with {:ok, namespaces} <- discover_namespaces(hosts, admin_port, tenant, namespace) do
      Enum.reduce_while(namespaces, {:ok, []}, fn {tenant, namespace}, {:ok, acc} ->
        with {:ok, partitioned_topics} <-
               lookup_partitioned_topics(hosts, admin_port, tenant, namespace),
             {:ok, topics} <- lookup_topics(hosts, admin_port, tenant, namespace) do
          Enum.reduce_while(partitioned_topics ++ topics, {:ok, []}, fn topic, {:ok, acc} ->
            case Topic.parse(topic) do
              {:ok, %Topic{} = topic} ->
                if topic_name == topic.topic do
                  {:cont, {:ok, [Topic.to_name(topic) | acc]}}
                else
                  {:cont, {:ok, acc}}
                end

              {:error, err} ->
                {:halt, {:error, err}}
            end
          end)
        end
        |> case do
          {:ok, topics} ->
            {:cont, {:ok, acc ++ topics}}

          {:error, err} ->
            {:halt, {:error, err}}
        end
      end)
    end
  end

  def discover_clusters(host, admin_port) when is_binary(host) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/clusters"
    }

    with {:ok, 200, _, client_ref} <-
           :hackney.get(URI.to_string(url), [], "", follow_redirect: true),
         {:ok, body} <- :hackney.body(client_ref) do
      Jason.decode(body)
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end

  def discover_clusters(hosts, admin_port) when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case discover_clusters(host, admin_port) do
        {:ok, clusters} -> {:halt, {:ok, clusters}}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end

  def create_tenant(host, admin_port, tenant, clusters) when is_binary(host) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/tenants/#{tenant}"
    }

    body = Jason.encode!(%{"allowedClusters" => clusters})

    with {:ok, status, _, _} when status in [204, 409] <-
           :hackney.put(URI.to_string(url), [{"Content-Type", "application/json"}], body,
             follow_redirect: true
           ) do
      :ok
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end

  def create_tenant(hosts, admin_port, tenant, clusters) when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case create_tenant(host, admin_port, tenant, clusters) do
        :ok -> {:halt, :ok}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end

  def create_namespace(host, admin_port, tenant, namespace, policies \\ %{})

  def create_namespace(host, admin_port, tenant, namespace, policies) when is_binary(host) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/namespaces/#{tenant}/#{namespace}"
    }

    body = Jason.encode!(policies)

    with {:ok, status, _, _} when status in [204, 409] <-
           :hackney.put(URI.to_string(url), [{"Content-Type", "application/json"}], body,
             follow_redirect: true
           ) do
      :ok
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end

  def create_namespace(hosts, admin_port, tenant, namespace, policies) when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case create_namespace(host, admin_port, tenant, namespace, policies) do
        :ok -> {:halt, :ok}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end

  def create_topic(host, admin_port, tenant, namespace, topic) when is_binary(host) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/persistent/#{tenant}/#{namespace}/#{topic}"
    }

    with {:ok, status, _, _} when status in [204, 409] <-
           :hackney.put(URI.to_string(url), [{"Content-Type", "application/json"}], "",
             follow_redirect: true
           ) do
      :ok
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end

  def create_topic(hosts, admin_port, tenant, namespace, topic) when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case create_topic(host, admin_port, tenant, namespace, topic) do
        :ok -> {:halt, :ok}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end

  def create_partitioned_topic(host, admin_port, tenant, namespace, topic, partitions)
      when is_binary(host) do
    url = %URI{
      scheme: "http",
      host: host,
      port: admin_port,
      path: "/admin/v2/persistent/#{tenant}/#{namespace}/#{topic}/partitions"
    }

    with {:ok, status, _, _} when status in [204, 409] <-
           :hackney.put(
             URI.to_string(url),
             [{"Content-Type", "application/json"}],
             "#{partitions}",
             follow_redirect: true
           ) do
      :ok
    else
      {:ok, _, _, client_ref} ->
        {:ok, body} = :hackney.body(client_ref)
        {:error, body}

      err ->
        err
    end
  end

  def create_partitioned_topic(hosts, admin_port, tenant, namespace, topic, partitions)
      when is_list(hosts) do
    hosts
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, :no_brokers_available}, fn host, _ ->
      case create_partitioned_topic(host, admin_port, tenant, namespace, topic, partitions) do
        :ok -> {:halt, :ok}
        {:error, err} -> {:cont, {:error, err}}
      end
    end)
  end
end
