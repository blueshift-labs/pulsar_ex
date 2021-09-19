defmodule PulsarEx.DefaultPassiveConsumer do
  use PulsarEx.Consumer, passive_mode: true
end
