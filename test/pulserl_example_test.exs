defmodule PulserlExampleTest do
  use ExUnit.Case
  doctest PulserlExample

  test "greets the world" do
    assert PulserlExample.hello() == :world
  end
end
