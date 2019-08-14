defmodule Kafkex.Consumer do
  use GenStage

  require Logger

  @latest_offset -1
  @earliest_offset -2
  @check_demand_interval 10
  @heartbeat_interval 1000
  @member_assignment_version 0
  @retention_time_ms 604_800_000

  def start_link({seed_brokers, topic, group_id}) do
    start_link({seed_brokers, topic, group_id, %{}})
  end

  def start_link({seed_brokers, topic, group_id, options}) do
    options = Map.put(options, :auto_commit, Map.get(options, :auto_commit, true))

    if options[:name] do
      GenStage.start_link(__MODULE__, {seed_brokers, topic, group_id, options},
        name: options[:name]
      )
    else
      GenStage.start_link(__MODULE__, {seed_brokers, topic, group_id, options})
    end
  end

  def commit(pid, messages) do
    latest_offsets = offsets_from_messages(messages)

    send(pid, {:commit, latest_offsets})
  end

  def init({seed_brokers, topic, group_id, options}) do
    {:ok, client} = Kafkex.Client.start_link(seed_brokers)
    join_group(client, topic, group_id, "", options)
  end

  def handle_demand(demand, state) do
    Logger.debug("[#{__MODULE__}][#{inspect(self())}] received fresh demand: #{demand}")
    fill_demand(demand, state)
  end

  def handle_info({:check_demand}, %{pending_demand: pending_demand} = state)
      when pending_demand > 0 do
    Logger.debug("[#{__MODULE__}][#{inspect(self())}] found pending demand: #{pending_demand}")
    send(self(), {:check_demand})
    fill_demand(0, state)
  end

  def handle_info({:check_demand}, state) do
    :timer.send_after(@check_demand_interval, {:check_demand})
    {:noreply, [], state}
  end

  def handle_info(
        {:commit, latest_offsets},
        %{
          client: client,
          group_id: group_id,
          generation_id: generation_id,
          member_id: member_id,
          offsets: offsets,
          topic: topic
        } = state
      ) do
    new_offsets =
      if map_size(latest_offsets) > 0 and latest_offsets != offsets do
        Kafkex.Client.offset_commit(
          client,
          group_id,
          generation_id,
          member_id,
          @retention_time_ms,
          [build_commit_options(latest_offsets, topic)]
        )

        Logger.debug(
          "[#{__MODULE__}][#{inspect(self())}] committed offsets: #{inspect(latest_offsets)}"
        )

        Map.merge(offsets, latest_offsets)
      else
        offsets
      end

    {:noreply, [], %{state | offsets: new_offsets}}
  end

  def handle_info(
        {:EXIT, _from, reason},
        %{client: client, topic: topic, group_id: group_id, member_id: member_id},
        options: options
      ) do
    Logger.warn("[#{__MODULE__}][#{inspect(self())}] process died: #{inspect(reason)}")
    {:producer, new_state} = join_group(client, topic, group_id, member_id, options)
    {:noreply, [], new_state}
  end

  defp join_group(client, topic, group_id, member_id, options) do
    Logger.debug(
      "[#{__MODULE__}][#{inspect(self())}] joining group: #{inspect({topic, group_id, member_id})}"
    )

    {:ok, response} = Kafkex.Client.join_group(client, topic, group_id, member_id)

    case response do
      {:ok, join_response} ->
        Logger.info(
          "[#{__MODULE__}][#{inspect(self())}] join group success: #{inspect(join_response)}"
        )

        sync_group(client, topic, group_id, join_response, options)

      {:error, error} ->
        Logger.error("[#{__MODULE__}][#{inspect(self())}] join group error: #{inspect(error)}")
        join_group(client, topic, group_id, member_id, options)
    end
  end

  defp sync_group(client, topic, group_id, join_response, options) do
    group_assignment = assemble_group_assignment(client, topic, join_response)

    Logger.debug(
      "[#{__MODULE__}][#{inspect(self())}] syncing group: #{inspect(group_assignment)}"
    )

    {:ok, response} =
      Kafkex.Client.sync_group(
        client,
        group_id,
        join_response.generation_id,
        join_response.member_id,
        group_assignment
      )

    case response do
      {:ok, sync_response} ->
        Logger.info(
          "[#{__MODULE__}][#{inspect(self())}] sync group success: #{inspect(sync_response)}"
        )

        finish_init(client, topic, group_id, join_response, sync_response, options)

      {:error, error} ->
        Logger.error("[#{__MODULE__}][#{inspect(self())}] sync group error: #{inspect(error)}")
        join_group(client, topic, group_id, join_response.member_id, options)
    end
  end

  defp finish_init(client, topic, group_id, join_response, sync_response, options) do
    Process.flag(:trap_exit, true)

    offsets = fetch_offsets(client, topic, group_id, sync_response, options)

    spawn_link(__MODULE__, :heartbeat, [
      client,
      group_id,
      join_response.generation_id,
      join_response.member_id
    ])

    send(self(), {:check_demand})

    {:producer,
     %{
       client: client,
       topic: topic,
       group_id: group_id,
       member_id: join_response.member_id,
       generation_id: join_response.generation_id,
       member_assignment: sync_response,
       offsets: offsets,
       highwater: offsets,
       options: options,
       pending_demand: 0,
       pending_events: []
     }}
  end

  def heartbeat(client, group_id, generation_id, member_id) do
    {:ok, :NONE} = Kafkex.Client.heartbeat(client, group_id, generation_id, member_id)

    :timer.sleep(@heartbeat_interval)

    heartbeat(client, group_id, generation_id, member_id)
  end

  def fill_demand(
        current_demand,
        %{
          pending_demand: pending_demand,
          pending_events: pending_events,
          options: options
        } = state
      ) do
    should_fill_by_fetch = length(pending_events) == 0
    new_demand = current_demand + pending_demand

    {ready_events, pending_events} =
      if should_fill_by_fetch do
        fill_by_fetch(current_demand, pending_demand, new_demand, state)
      else
        fill_by_dequeue(current_demand, pending_demand, new_demand, pending_events)
      end

    Logger.debug(
      "[#{__MODULE__}][#{inspect(self())}] retreived #{length(ready_events)} events, with #{
        length(pending_events)
      } pending"
    )

    latest_offsets = offsets_from_messages(ready_events)

    highwater =
      if should_fill_by_fetch and length(ready_events) > 0 do
        latest_offsets
      else
        state[:highwater]
      end

    if options[:auto_commit] do
      commit(self(), ready_events)
    end

    {:noreply, ready_events,
     %{
       state
       | highwater: highwater,
         pending_demand: new_demand - length(ready_events),
         pending_events: pending_events
     }}
  end

  defp fill_by_fetch(current_demand, pending_demand, new_demand, state) do
    Logger.debug(
      "[#{__MODULE__}][#{inspect(self())}] attempting to fill demand by fetching, current: #{
        current_demand
      }, pending: #{pending_demand}"
    )

    topic_responses = fetch(new_demand, state)

    events_from_response(new_demand, topic_responses)
  end

  defp fill_by_dequeue(current_demand, pending_demand, new_demand, pending_events) do
    Logger.debug(
      "[#{__MODULE__}][#{inspect(self())}] attempting to fill demand by dequeueing, current:: #{
        current_demand
      }, pending: #{pending_demand}"
    )

    dequeue_pending_events(new_demand, pending_events)
  end

  defp assemble_group_assignment(client, topic, %Kafkex.Protocol.JoinGroup.Response{
         leader_id: me,
         member_id: me,
         members: members
       }) do
    {:ok, %{leaders: %{^topic => leaders}}} = Kafkex.Client.metadata(client)

    partitions = leaders |> Map.keys()

    member_ids =
      members
      |> Enum.map(& &1.member_id)
      |> Stream.cycle()
      |> Enum.take(length(partitions))

    Enum.zip(member_ids, partitions)
    |> Enum.group_by(fn {m, _} -> m end, fn {_, p} -> p end)
    |> Enum.map(fn {member_id, partitions} ->
      {member_id,
       %Kafkex.Protocol.SyncGroup.MemberAssignment{
         version: @member_assignment_version,
         partition_assignments: [
           %Kafkex.Protocol.SyncGroup.PartitionAssignment{topic: topic, partitions: partitions}
         ]
       }}
    end)
  end

  defp assemble_group_assignment(_, _, _), do: []

  defp fetch_offsets(client, topic, group_id, sync_response, options) do
    partitions =
      sync_response.partition_assignments
      |> Enum.find(&(&1.topic == topic))
      |> Map.get(:partitions)

    if options[:from_beginning] do
      fetch_topic_offsets(client, topic, partitions, @earliest_offset)
    else
      fetch_group_offsets(client, topic, partitions, group_id) ||
        fetch_topic_offsets(client, topic, partitions)
    end
  end

  defp fetch_group_offsets(client, topic, partitions, group_id) do
    {:ok, response} =
      Kafkex.Client.offset_fetch(client, group_id, [[topic: topic, partitions: partitions]])

    topic_partitions = response.topic_partitions |> Enum.find(&(&1.topic == topic))

    is_new_group = topic_partitions.partitions |> Enum.all?(&(&1.offset == -1))

    unless is_new_group do
      topic_partitions.partitions
      |> Enum.map(&{&1.partition, &1.offset})
      |> Enum.into(%{})
    end
  end

  defp fetch_topic_offsets(client, topic, partitions) do
    fetch_topic_offsets(client, topic, partitions, @latest_offset)
  end

  defp fetch_topic_offsets(client, topic, partitions, offset) do
    Logger.debug(
      "[#{__MODULE__}][#{inspect(self())}] fetching topic offsets for: #{inspect(offset)}"
    )

    {:ok, response} =
      Kafkex.Client.offsets(client, [[topic: topic, partitions: partitions]], offset)

    topic_partitions = response.topic_partitions |> Enum.find(&(&1.topic == topic))

    topic_partitions.partitions
    |> Enum.map(&{&1.partition, hd(&1.offsets)})
    |> Enum.into(%{})
  end

  defp dequeue_pending_events(demand, pending_events),
    do: dequeue_pending_events(demand, pending_events, [])

  defp dequeue_pending_events(0, pending_events, events), do: {events, pending_events}
  defp dequeue_pending_events(_demand, [], events), do: {events, []}

  defp dequeue_pending_events(demand, [event | rest], events),
    do: dequeue_pending_events(demand - 1, rest, [event | events])

  defp fetch(_demand, %{client: client} = state) do
    topic_partitions = build_fetch_options(state)

    Logger.debug(
      "[#{__MODULE__}][#{inspect(self())}] fetching with topic partitions: #{
        inspect(topic_partitions)
      }"
    )

    {:ok, %Kafkex.Protocol.Fetch.Response{topic_responses: topic_responses}} =
      Kafkex.Client.fetch(client, topic_partitions)

    topic_responses
  end

  defp build_fetch_options(%{
         member_assignment: member_assignment,
         highwater: highwater,
         topic: topic
       }) do
    member_assignment.partition_assignments
    |> Enum.filter(&(&1.topic == topic))
    |> Enum.map(fn partition_assignment ->
      partitions =
        partition_assignment.partitions
        |> Enum.map(fn partition ->
          [partition: partition, offset: highwater[partition]]
        end)

      [topic: topic, partitions: partitions]
    end)
  end

  defp build_commit_options(latest_offsets, topic) do
    partitions =
      latest_offsets
      |> Enum.map(fn {partition, offset} ->
        [partition: partition, offset: offset, metadata: nil]
      end)

    [topic: topic, partitions: partitions]
  end

  defp events_from_response(demand, topic_responses) do
    topic_responses
    |> Enum.flat_map(fn topic_response ->
      topic_response.partitions
      |> Enum.flat_map(fn partition -> partition.messages end)
    end)
    |> Enum.split(demand)
  end

  defp offsets_from_messages(messages) do
    messages
    |> Enum.group_by(fn message -> message.partition end)
    |> Enum.map(fn {partition, messages} ->
      max_offset =
        messages
        |> Enum.map(fn message -> message.offset end)
        |> Enum.max()

      {partition, max_offset + 1}
    end)
    |> Enum.filter(fn {_partition, offset} -> offset > 0 end)
    |> Enum.into(%{})
  end
end
