defmodule PulsarEx.Bitset do
  defstruct size: 0, data: <<>>

  alias __MODULE__

  @word_length 64

  def new(size) do
    bits = bits(size)
    %Bitset{size: size, data: <<0::size(bits)>>}
  end

  def set?(%Bitset{size: size}, pos) when pos >= size, do: false

  def set?(%Bitset{data: data}, pos) do
    <<_::size(pos), bit::1, _::bits>> = data
    bit == 1
  end

  def set(%Bitset{size: size, data: data} = bitset, pos) when pos >= 0 and pos < size do
    <<prefix::size(pos), _::1, suffix::bits>> = data
    %{bitset | data: <<prefix::size(pos), 1::1, suffix::bits>>}
  end

  def flip(%Bitset{size: size, data: data} = bitset) do
    %{bitset | data: flip_bits(data, 0, size, <<>>)}
  end

  def to_words(%Bitset{data: data}) do
    to_words(data, [])
  end

  def from_words(words, size), do: %Bitset{size: size, data: from_words(words)}

  defp from_words([]), do: <<>>

  defp from_words([word | words]) do
    reverse_word(<<word::@word_length>>) <> from_words(words)
  end

  defp to_words(<<>>, acc), do: Enum.reverse(acc)

  defp to_words(<<bits::@word_length, data::bits>>, acc) do
    <<x::big-signed-integer-size(@word_length)>> = reverse_word(<<bits::@word_length>>)
    to_words(data, [x | acc])
  end

  defp flip_bits(data, index, size, acc) when index >= size do
    <<acc::bits, data::bits>>
  end

  defp flip_bits(<<0::1, rest::bits>>, index, size, acc) do
    flip_bits(rest, index + 1, size, <<acc::bits, 1::1>>)
  end

  defp flip_bits(<<1::1, rest::bits>>, index, size, acc) do
    flip_bits(rest, index + 1, size, <<acc::bits, 0::1>>)
  end

  defp reverse_word(<<_::@word_length>> = data), do: reverse_word(data, <<>>)

  defp reverse_word(<<>>, acc), do: acc

  defp reverse_word(<<bit::1, rest::bits>>, acc) do
    reverse_word(rest, <<bit::1, acc::bits>>)
  end

  defp words(size), do: div(size + @word_length - 1, @word_length)

  defp bits(size), do: words(size) * @word_length
end
