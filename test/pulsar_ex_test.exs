defmodule PulsarExTest do
  use ExUnit.Case
  doctest PulsarEx

  test "greets the world" do
    assert PulsarEx.hello() == :world
  end
end
