defmodule Kafkex.Protocol.Broker do
  defstruct node_id: 0, host: "", port: 0

  def build(
        <<node_id::32, host_length::16, host::size(host_length)-binary, port::32, rest::binary>>
      ) do
    {%Kafkex.Protocol.Broker{node_id: node_id, host: host, port: port}, rest}
  end
end
