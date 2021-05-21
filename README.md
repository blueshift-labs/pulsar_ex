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
