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

stream = (1..201)|>Task.async_stream(fn x ->     
  PulsarEx.DefaultWorker.enqueue_job_async(:test, %{message: "hello-#{x}"})
end, max_concurrency: 100)

Enum.to_list(stream)


messages = PulsarEx.poll("persistent://public/default/2.json", "test", PulsarEx.DefaultPassiveConsumer)
