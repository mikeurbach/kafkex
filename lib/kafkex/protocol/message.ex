defmodule Kafkex.Protocol.Message do
  import Kafkex.Protocol

  @magic_byte 1
  @attributes 0

  def build([]), do: <<>>
  def build([{key,value}|rest]) do
    core = << @magic_byte :: 8 >> <> << @attributes :: 8 >> <> << timestamp() :: 64 >> <> build_item(key, 32) <> build_item(value, 32)
    crc = :erlang.crc32(core)
    message = << crc :: 32 >> <> core
    << 0 :: 64 >> <> << byte_size(message) :: 32 >> <> message <> build(rest)
  end
  def build([value|rest]), do: build([{nil,value}|rest])

  defp timestamp(), do: DateTime.utc_now |> DateTime.to_unix(:milliseconds)
end
