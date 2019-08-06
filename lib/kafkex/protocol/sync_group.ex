defmodule Kafkex.Protocol.SyncGroup do
  import Kafkex.Protocol

  defmodule PartitionAssignment do
    defstruct topic: "", partitions: []

    def build(%PartitionAssignment{topic: topic, partitions: partitions}, rest) do
      rest <>
        build_item(topic) <>
        <<length(partitions)::32>> <>
        Enum.reduce(partitions, <<>>, fn partition, others -> others <> <<partition::32>> end)
    end

    def parse(
          <<topic_length::16, topic::size(topic_length)-binary, partitions_length::32,
            rest::binary>>
        ) do
      {partitions, rest} =
        parse_list(partitions_length, rest, fn <<partition::32, rest::binary>> ->
          {partition, rest}
        end)

      {%PartitionAssignment{topic: topic, partitions: Enum.reverse(partitions)}, rest}
    end
  end

  defmodule MemberAssignment do
    defstruct version: -1, partition_assignments: [], data: ""

    def build(%MemberAssignment{
          version: version,
          partition_assignments: partition_assignments,
          data: data
        }) do
      build_item(
        <<version::16>> <>
          <<length(partition_assignments)::32>> <>
          Enum.reduce(partition_assignments, <<>>, &PartitionAssignment.build/2) <>
          build_item(data, 32),
        32
      )
    end

    def parse(<<version::16, partition_assignment_length::32, rest::binary>>) do
      {partition_assignments, <<_data_length::32, data::binary>>} =
        parse_list(
          partition_assignment_length,
          rest,
          &Kafkex.Protocol.SyncGroup.PartitionAssignment.parse/1
        )

      %MemberAssignment{
        version: version,
        partition_assignments: partition_assignments,
        data: data
      }
    end
  end

  defmodule Request do
    @api_key 14
    @api_version 0

    defstruct group_id: "", generation_id: -1, member_id: "", group_assignment: []

    def build(correlation_id, client_id,
          group_id: group_id,
          generation_id: generation_id,
          member_id: member_id,
          group_assignment: group_assignment
        ) do
      build_headers(@api_key, @api_version, correlation_id, client_id) <>
        build_item(group_id) <>
        <<generation_id::32>> <>
        build_item(member_id) <>
        <<length(group_assignment)::32>> <> build_group_assignment(group_assignment)
    end

    defp build_group_assignment([]), do: <<>>

    defp build_group_assignment([{member_id, member_assignment} | rest]) do
      build_item(member_id) <>
        MemberAssignment.build(member_assignment) <> build_group_assignment(rest)
    end
  end

  defmodule Response do
    defstruct member_assignment: nil

    def parse(
          {:ok,
           <<correlation_id::32, sync_error_code::16, _member_assignment_size::32,
             member_assignment::binary>>}
        ) do
      case error_code(sync_error_code) do
        :NONE -> {correlation_id, {:ok, MemberAssignment.parse(member_assignment)}}
        error -> {correlation_id, {:error, error}}
      end
    end
  end
end
