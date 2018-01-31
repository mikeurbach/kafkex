defmodule Kafkex.Protocol.Message do
  import Kafkex.Protocol

  require Logger

  @magic_byte 1
  @attributes 0
  @base_size_v0 26 # bytes for v0 message metadata
  @base_size_v1 34 # bytes for v1 message metadata

  defstruct partition: -1, offset: -1, timestamp: -1, key: "", value: ""

  def build([]), do: <<>>
  def build([{key,value}|rest]) do
    core = << @magic_byte :: 8 >> <> << @attributes :: 8 >> <> << timestamp() :: 64 >> <> build_item(key, 32) <> build_item(value, 32)
    crc = :erlang.crc32(core)
    message = << crc :: 32 >> <> core
    << 0 :: 64 >> <> << byte_size(message) :: 32 >> <> message <> build(rest)
  end
  def build([value|rest]), do: build([{nil,value}|rest])

  def parse(0, rest, items), do: {items, rest}

  def parse(bytes_remaining, << offset :: 64, _size :: 32, _crc :: 32, 0 :: 8, _attributes :: 8, key_size :: 32, rest :: binary>>, items) do
    parse_helper(@base_size_v0, bytes_remaining, offset, key_size, rest, items)
  end

  def parse(bytes_remaining, << offset :: 64, _size :: 32, _crc :: 32, 1 :: 8, _attributes :: 8, ts :: 64, key_size :: 32-signed, rest :: binary>>, items) do
    parse_helper(@base_size_v1, bytes_remaining, offset, key_size, rest, items, ts)
  end

  # from: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
  # As an optimization the server is allowed to return a partial message at the end of the message set. Clients should handle this case.
  def parse(bytes_remaining, rest, items) do
    << extra :: size(bytes_remaining)-binary, new_rest :: binary >> = rest
    Logger.warn("[#{__MODULE__}][#{inspect(self())}] found #{bytes_remaining} bytes remaining! extra: #{inspect(extra, limit: :infinity)}")
    {items, new_rest}
  end

  defp parse_helper(base_size, bytes_remaining, offset, key_size, binary, items, ts \\ nil) do
    {key, << value_size :: 32-signed, rest :: binary >>} = parse_nullable(key_size, binary)

    if value_size <= byte_size(rest) do
      {value, << new_rest :: binary >>} = parse_nullable(value_size, rest)

      bytes_parsed = base_size + byte_size(key || <<>>) + byte_size(value || <<>>)

      parse(bytes_remaining - bytes_parsed, new_rest, [%Kafkex.Protocol.Message{offset: offset, timestamp: ts, key: key, value: value} | items])
    else
      # Logger.warn("[#{__MODULE__}][#{inspect(self())}] expected #{value_size}, only #{byte_size(rest)} bytes remaining! extra: #{inspect(rest, limit: :infinity)}")
      {items, <<>>}
    end
  end

  defp timestamp(), do: DateTime.utc_now |> DateTime.to_unix(:milliseconds)
end
