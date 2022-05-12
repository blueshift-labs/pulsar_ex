# PulsarEx

## Installation

##### You'll notice you need to pull in a modified fork of pulserl for this package to work. (Hex only allows other hex packages as dependencies and the pulserl dep isn't ready to be published)

```elixir
def deps do
  [
    {:pulsar_ex, "~> 0.10.23"}
  ]
end
```


PulsarEx.produce("persistent://public/default/10000.json", "test")

defmodule TestWorker do
  use PulsarEx.Worker,
    otp_app: :pulsar_ex,
    topic: "persistent://public/default/test.json",
    subscription: "test",
    jobs: [:backfill_account, :backfill_partition, :backfill_collection, :backfill_user]

  def handle_job(job, job_state) do
    IO.inspect(job)
    IO.inspect(job_state)
    :ok
  end
end
