defmodule PulserlExample.Consumer do
  @moduledoc false
  use GenServer

  alias PulserlExample.{ConsumerRegistry}

  def start_link([topic, subscription, opts]) do
    GenServer.start_link(__MODULE__, [topic, subscription, opts])
  end

  @impl true
  def init([topic, subscription, opts]) do
    Process.flag(:trap_exit, true)

    {:ok, pid} =
      :pulserl_instance_registry.get_consumer(topic, Keyword.get(opts, :consumer_opts, []))

    {:ok, %{pid: pid}}
  end

  # @impl true
  # def handle_info(
  #       {:neutron_msg, msg_id_ref, _topic, _partition_key, _ordering_key, _publish_ts, _event_ts,
  #        redelivery_count, _properties, _payload} = msg,
  #       %{
  #         consumer_opts: consumer_opts,
  #         callback_module: callback_module,
  #         consumer_ref: consumer_ref
  #       } = state
  #     ) do
  # end

  @impl true
  def terminate(_reason, %{pid: pid} = state) do
    :ok = :pulserl_consumer.close(pid)
    state
  end
end
