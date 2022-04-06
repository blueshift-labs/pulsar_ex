defmodule PulsarEx.Executor do
  use GenServer

  def exec(pid, handler, timeout \\ 5_000) do
    GenServer.call(pid, {:exec, handler}, timeout)
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil)
  end

  @impl true
  def init(_) do
    {:ok, nil}
  end

  @impl true
  def handle_call({:exec, handler}, _from, state) do
    {:reply, handler.(), state}
  end
end
