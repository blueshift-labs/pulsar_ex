defmodule PulsarEx.SignalHandler do
  @behaviour :gen_event

  require Logger

  @shutdown_timeout 5000

  def start_link() do
    timeout = Application.get_env(:pulsar_ex, :shutdown_timeout, @shutdown_timeout)

    :ok =
      :gen_event.swap_sup_handler(
        :erl_signal_server,
        {:erl_signal_handler, []},
        {__MODULE__, timeout}
      )

    :ignore
  end

  def init({timeout, _}), do: {:ok, timeout}

  def handle_event(:sigterm, timeout) do
    Logger.debug("Application received TERM signal #{:sigterm}, shutting down...")

    PulsarEx.Application.shutdown!()

    children = Supervisor.which_children(PulsarEx.Supervisor)
    Process.send_after(self(), {:stop, children}, timeout)
    {:ok, timeout}
  end

  def handle_event(signal, timeout) do
    Logger.error("Application received unexpected signal #{inspect(signal)}, stopping...")

    :init.stop()
    {:ok, timeout}
  end

  def handle_info(:stop, timeout) do
    :init.stop()
    {:ok, timeout}
  end

  def handle_info({:stop, []}, timeout) do
    Process.send(self(), :stop, [])
    {:ok, timeout}
  end

  def handle_info({:stop, [{_, :undefined, _, _} | children]}, timeout) do
    Process.send(self(), {:stop, children}, [])
    {:ok, timeout}
  end

  def handle_info({:stop, [{id, _, _, _} | children]}, timeout) do
    Supervisor.terminate_child(PulsarEx.Supervisor, id)
    Process.send(self(), {:stop, children}, [])
    {:ok, timeout}
  end

  def handle_info(_, timeout), do: {:ok, timeout}

  def handle_call(_, timeout), do: {:ok, :ok, timeout}
end
