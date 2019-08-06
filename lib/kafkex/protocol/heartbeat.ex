defmodule Kafkex.Protocol.Heartbeat do
  import Kafkex.Protocol

  defmodule Request do
    @api_key 12
    @api_version 0

    defstruct group_id: "", generation_id: -1, member_id: ""

    def build(correlation_id, client_id,
          group_id: group_id,
          generation_id: generation_id,
          member_id: member_id
        ) do
      build_headers(@api_key, @api_version, correlation_id, client_id) <>
        build_item(group_id) <> <<generation_id::32>> <> build_item(member_id)
    end
  end

  defmodule Response do
    def parse({:ok, <<correlation_id::32, heartbeat_error_code::16>>}) do
      {correlation_id, error_code(heartbeat_error_code)}
    end
  end
end
