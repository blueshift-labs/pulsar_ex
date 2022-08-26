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

## Usage
### Creating Producer
```
PulsarEx.produce("persistent://public/default/test.json", "test")
```

### Creating Consumer
```
defmodule TestWorker do
  use PulsarEx.Worker,
    otp_app: :pulsar_ex,
    topic: "persistent://public/default/test.json",
    subscription: "test"

  def handle_job(%JobState{} = message) do
    IO.inspect(message)
    :ok
  end
end
```