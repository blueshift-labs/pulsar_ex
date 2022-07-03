defmodule PulsarEx.Executor do
  use GenServer

  @num_executors 5

  def exec(pid, handler, timeout \\ 5_000) do
    GenServer.call(pid, {:exec, handler}, timeout)
  end

  def start_link([]) do
    GenServer.start_link(__MODULE__, [])
  end

  @impl true
  def init([]) do
    {:ok, []}
  end

  @impl true
  def handle_call({:exec, handler}, _from, []) do
    {:reply, handler.(), []}
  end

  def pool_spec(cluster_opts) do
    cluster = Keyword.get(cluster_opts, :cluster, :default)
    num_executors = Keyword.get(cluster_opts, :num_executors, @num_executors)
    executors_overflow = round(num_executors * 0.2)
    name = name(cluster)

    executors_opts = [
      name: {:local, name},
      worker_module: __MODULE__,
      size: num_executors,
      max_overflow: executors_overflow,
      strategy: :fifo
    ]

    :poolboy.child_spec(name, executors_opts)
  end

  def name(cluster) do
    String.to_atom("#{__MODULE__}.#{cluster}")
  end
end
