defmodule Kafkex.Protocol.Message do
  import Kafkex.Protocol

  @magic_byte 1
  @attributes 0
  @base_size_v0 26 # bytes for v0 message metadata
  @base_size_v1 34 # bytes for v1 message metadata

  defstruct offset: -1, timestamp: -1, key: "", value: ""

  def build([]), do: <<>>
  def build([{key,value}|rest]) do
    core = << @magic_byte :: 8 >> <> << @attributes :: 8 >> <> << timestamp() :: 64 >> <> build_item(key, 32) <> build_item(value, 32)
    crc = :erlang.crc32(core)
    message = << crc :: 32 >> <> core
    << 0 :: 64 >> <> << byte_size(message) :: 32 >> <> message <> build(rest)
  end
  def build([value|rest]), do: build([{nil,value}|rest])

  def parse(0, rest, items), do: {items, rest}
  def parse(bytes_remaining, << offset :: 64, _size :: 32, _crc :: 32, 0 :: 8, _attributes :: 8, key_size :: 32, key :: size(key_size)-binary, value_size :: 32, value :: size(value_size)-binary, rest :: binary>>, items) do
    bytes_parsed = @base_size_v0 + key_size + value_size
    parse(bytes_remaining - bytes_parsed, rest, [%Kafkex.Protocol.Message{offset: offset, key: key, value: value} | items])
  end
  def parse(bytes_remaining, << offset :: 64, _size :: 32, _crc :: 32, 1 :: 8, _attributes :: 8, timestamp :: 64, key_size :: 32, key :: size(key_size)-binary, value_size :: 32, value :: size(value_size)-binary, rest :: binary>>, items) do
    bytes_parsed = @base_size_v1 + key_size + value_size
    parse(bytes_remaining - bytes_parsed, rest, [%Kafkex.Protocol.Message{offset: offset, timestamp: timestamp, key: key, value: value} | items])
  end

  defp timestamp(), do: DateTime.utc_now |> DateTime.to_unix(:milliseconds)
end
