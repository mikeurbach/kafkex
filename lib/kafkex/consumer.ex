defmodule Kafkex.Consumer do
  use GenServer

  require Logger

  @heartbeat_interval 1000
  @member_assignment_version 0

  def start_link({seed_brokers, topic, group_id}) do
    GenServer.start_link(__MODULE__, {seed_brokers, topic, group_id})
  end

  def init({seed_brokers, topic, group_id}) do
    {:ok, client} = Kafkex.Client.start_link(seed_brokers)
    join_group(client, topic, group_id, "")
  end

  def handle_info({:EXIT, _from, reason}, %{client: client, topic: topic, group_id: group_id, member_id: member_id}) do
    Logger.warn("[#{__MODULE__}][#{inspect(self())}] process died: #{inspect(reason)}")
    {:ok, state} = join_group(client, topic, group_id, member_id)
    {:noreply, state}
  end

  defp join_group(client, topic, group_id, member_id) do
    Logger.debug("[#{__MODULE__}][#{inspect(self())}] joining group: #{inspect({topic, group_id, member_id})}")

    {:ok, response} = Kafkex.Client.join_group(client, topic, group_id, member_id)

    case response do
      {:ok, join_response} ->
        Logger.info("[#{__MODULE__}][#{inspect(self())}] join group success: #{inspect(join_response)}")
        sync_group(client, topic, group_id, join_response)
      {:error, error} ->
        Logger.error("[#{__MODULE__}][#{inspect(self())}] join group error: #{inspect(error)}")
        join_group(client, topic, group_id, member_id)
    end
  end

  defp sync_group(client, topic, group_id, join_response) do
    group_assignment = assemble_group_assignment(client, topic, join_response)

    Logger.debug("[#{__MODULE__}][#{inspect(self())}] syncing group: #{inspect(group_assignment)}")

    {:ok, response} = Kafkex.Client.sync_group(client, group_id, join_response.generation_id, join_response.member_id, group_assignment)

    case response do
      {:ok, sync_response} ->
        Logger.info("[#{__MODULE__}][#{inspect(self())}] sync group success: #{inspect(sync_response)}")
        finish_init(client, topic, group_id, join_response, sync_response)
      {:error, error} ->
        Logger.error("[#{__MODULE__}][#{inspect(self())}] sync group error: #{inspect(error)}")
        join_group(client, topic, group_id, join_response.member_id)
    end
  end

  defp finish_init(client, topic, group_id, join_response, sync_response) do
    Process.flag(:trap_exit, true)

    offsets = fetch_offsets(client, topic, group_id, sync_response)

    spawn_link(__MODULE__, :heartbeat, [client, group_id, join_response.generation_id, join_response.member_id])

    {:ok, %{client: client, topic: topic, group_id: group_id, member_id: join_response.member_id, member_assignment: sync_response, offsets: offsets}}
  end

  def heartbeat(client, group_id, generation_id, member_id) do
    {:ok, :NONE} = Kafkex.Client.heartbeat(client, group_id, generation_id, member_id)

    :timer.sleep(@heartbeat_interval)

    heartbeat(client, group_id, generation_id, member_id)
  end

  defp assemble_group_assignment(client, topic, %Kafkex.Protocol.JoinGroup.Response{leader_id: me, member_id: me, members: members}) do
    {:ok, %{leaders: %{^topic => leaders}}} = Kafkex.Client.metadata(client)

    partitions = leaders |> Map.keys

    member_ids = members
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
  defp assemble_group_assignment(_, _, _), do: []

  defp fetch_offsets(client, topic, group_id, sync_response) do
    partitions = sync_response.partition_assignments
    |> Enum.find(&(&1.topic == topic))
    |> Map.get(:partitions)

    fetch_group_offsets(client, topic, partitions, group_id) || fetch_topic_offsets(client, topic, partitions)
  end

  defp fetch_group_offsets(client, topic, partitions, group_id) do
    {:ok, response} = Kafkex.Client.offset_fetch(client, group_id, [[topic: topic, partitions: partitions]])

    topic_partitions = response.topic_partitions |> Enum.find(&(&1.topic == topic))

    is_new_group = topic_partitions.partitions |> Enum.all?(&(&1.offset == -1))

    unless is_new_group do
      topic_partitions.partitions
      |> Enum.map(&({&1.partition, &1.offset}))
      |> Enum.into(%{})
    end
  end

  defp fetch_topic_offsets(client, topic, partitions) do
    {:ok, response} = Kafkex.Client.offsets(client, [[topic: topic, partitions: partitions]])

    topic_partitions = response.topic_partitions |> Enum.find(&(&1.topic == topic))

    topic_partitions.partitions
    |> Enum.map(&({&1.partition, hd(&1.offsets)}))
    |> Enum.into(%{})
  end
end
