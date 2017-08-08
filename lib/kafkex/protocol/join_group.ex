defmodule Kafkex.Protocol.JoinGroup do
  import Kafkex.Protocol

  defmodule Request do
    @api_key 11
    @api_version 0
    @session_timeout 10_000
    @protocol_type "consumer"
    @protocol_name "roundrobin"
    @protocol_metadata_version 0
    @protocol_user_data nil

    defstruct group_id: "", session_timeout: 0, member_id: "", protocol_type: "", group_protocols: []

    def build(correlation_id, client_id, [topic: topic, group_id: group_id, member_id: member_id] = options) do
      session_timeout = options |> Keyword.get(:session_timeout, @session_timeout)
      protocol_type = options |> Keyword.get(:protocol_type, @protocol_type)
      protocol_name = options |> Keyword.get(:protocol_name, @protocol_name)
      protocol_metadata_version = options |> Keyword.get(:protocol_metadata_version, @protocol_metadata_version)
      protocol_user_data = options |> Keyword.get(:protocol_user_data, @protocol_user_data)

      build_headers(@api_key, @api_version, correlation_id, client_id) <> build_item(group_id) <> << session_timeout :: 32>> <> build_item(member_id) <> build_item(protocol_type) <> << 1 :: 32>> <> build_item(protocol_name) <> << protocol_metadata_version :: 32 >> <> build_list([topic]) <> build_item(protocol_user_data)
    end
  end

  defmodule Response do
    defstruct generation_id: 0, group_protocol: "", leader_id: "", member_id: "", members: []

    def parse({:ok, << correlation_id :: 32, join_error_code :: 16, generation_id :: 32, group_protocol_length :: 16, group_protocol :: size(group_protocol_length)-binary, leader_id_length :: 16, leader_id :: size(leader_id_length)-binary, member_id_length :: 16, member_id :: size(member_id_length)-binary, members_length :: 32, rest :: binary >>}) do
      case error_code(join_error_code) do
        :NONE ->
          {members, <<>>} = parse_list(members_length, rest, &Kafkex.Protocol.JoinGroup.Member.build/1)
          {correlation_id, {:ok, %Kafkex.Protocol.JoinGroup.Response{generation_id: generation_id, group_protocol: group_protocol, leader_id: leader_id, member_id: member_id, members: members}}}
        error -> {correlation_id, {:error, error}}
      end
    end
  end

  defmodule Member do
    defstruct member_id: "", member_metadata: ""

    def build(<< member_id_length :: 16, member_id :: size(member_id_length)-binary, member_metadata_length :: 32, member_metadata :: size(member_metadata_length)-binary, rest :: binary >>) do
      {%Kafkex.Protocol.JoinGroup.Member{member_id: member_id, member_metadata: member_metadata}, rest}
    end
  end
end
