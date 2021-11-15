# PulsarEx

**Not recommended for production just yet

## Installation

##### You'll notice you need to pull in a modified fork of pulserl for this package to work. (Hex only allows other hex packages as dependencies and the pulserl dep isn't ready to be published)

```elixir
def deps do
  [
    {
     :pulserl,
     git: "https://github.com/blueshift-labs/pulserl.git",
     tag: "0.1.3",
    },
    {:pulsar_ex, "~> 0.1"}
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

  def handle_job(job, params) do
    IO.inspect(job)
    IO.inspect(params)
    :ok
  end
end
