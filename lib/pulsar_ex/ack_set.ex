defmodule PulsarEx.AckSet do
  defstruct data: <<>>

  alias __MODULE__

  import Bitwise

  def new(size) do
    %AckSet{data: <<(1 <<< (size + 1)) - 1::size(size)>>}
  end

  def clear?(%AckSet{data: data}) do
    size = bit_size(data)
    data == <<0::size(size)>>
  end

  def set(%AckSet{data: data} = ack_set, pos) when pos >= 0 and pos < bit_size(data) do
    <<prefix::size(pos), _::1, suffix::bits>> = data
    %{ack_set | data: <<prefix::size(pos), 0::1, suffix::bits>>}
  end

  def and_set(%AckSet{data: a_data}, %AckSet{data: b_data})
      when bit_size(a_data) == bit_size(b_data) do
    %AckSet{data: and_bits(a_data, b_data, <<>>)}
  end

  defp and_bits(<<>>, <<>>, res), do: res

  defp and_bits(<<1::1, a::bits>>, <<1::1, b::bits>>, res) do
    and_bits(a, b, <<res::bits, 1::1>>)
  end

  defp and_bits(<<_::1, a::bits>>, <<_::1, b::bits>>, res) do
    and_bits(a, b, <<res::bits, 0::1>>)
  end
end
