defmodule Kafkex.Consumer do
  use GenServer

  @heartbeat_interval 1000
  @member_assignment_version 0

  def start_link({group_id, topic}) do
    GenServer.start_link(__MODULE__, {group_id, topic})
  end

  def init({group_id, topic}) do
    {:ok, join_resp} = Kafkex.Client.join_group(group_id, topic)

    group_assignment = assemble_group_assignment(join_resp, topic)

    {:ok, member_assignment} = Kafkex.Client.sync_group(group_id, join_resp.generation_id, join_resp.member_id, group_assignment)

    spawn_link(__MODULE__, :heartbeat, [group_id, join_resp.generation_id, join_resp.member_id])

    {:ok, %{member_assignment: member_assignment}}
  end

  def heartbeat(group_id, generation_id, member_id) do
    Kafkex.Client.heartbeat(group_id, generation_id, member_id)
    :timer.sleep(@heartbeat_interval)
    heartbeat(group_id, generation_id, member_id)
  end

  defp assemble_group_assignment(join_resp, topic) do
    {:ok, %{leaders: %{^topic => leaders}}} = Kafkex.Client.metadata

    partitions = leaders |> Map.keys

    member_ids = join_resp.members
    |> Enum.map(&(&1.member_id))
    |> Stream.cycle
    |> Enum.take(length(partitions))

    Enum.zip(member_ids, partitions)
    |> Enum.group_by(fn({m, _}) -> m end, fn({_, p}) -> p end)
    |> Enum.map(fn({member_id, partitions}) ->
      {member_id, %Kafkex.Protocol.SyncGroup.MemberAssignment{
        version: @member_assignment_version,
        partition_assignments: [%Kafkex.Protocol.SyncGroup.PartitionAssignment{topic: topic, partitions: partitions}]}}
    end)
  end
end
