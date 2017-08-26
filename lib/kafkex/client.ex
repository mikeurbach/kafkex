defmodule Kafkex.Client do
  use GenServer

  require Logger

  @client_id "kafkex"
  @socket_timeout_ms 30_000

  def start_link(seed_brokers) do
    GenServer.start_link(__MODULE__, seed_brokers)
  end

  def init(seed_brokers) do
    case fetch_metadata(seed_brokers) do
      {:ok, metadata} ->
        connections = build_connections(metadata)
        leaders = build_leaders(metadata)
        correlation_ids = initialize_correlation_ids(connections)

        {:ok, %{metadata: metadata, connections: connections, leaders: leaders, correlation_ids: correlation_ids, group_coordinators: %{}}}
      {:error, _} = error ->
        {:stop, error}
    end
  end

  def produce(pid, topic, partition, messages) do
    GenServer.call(pid, {:produce, topic, partition, messages}, @socket_timeout_ms)
  end

  def offsets(pid, topic_partitions, time \\ -1, max_offsets \\ 1) do
    GenServer.call(pid, {:offsets, topic_partitions, time, max_offsets})
  end

  def offset_fetch(pid, group_id, topic_partitions) do
    GenServer.call(pid, {:offset_fetch, group_id, topic_partitions})
  end

  def group_coordinator(pid, group_id) do
    GenServer.call(pid, {:group_coordinator, group_id}, @socket_timeout_ms)
  end

  def join_group(pid, topic, group_id, member_id) do
    GenServer.call(pid, {:join_group, topic, group_id, member_id}, @socket_timeout_ms)
  end

  def sync_group(pid, group_id, generation_id, member_id, group_assignment) do
    GenServer.call(pid, {:sync_group, group_id, generation_id, member_id, group_assignment}, @socket_timeout_ms)
  end

  def heartbeat(pid, group_id, generation_id, member_id) do
    GenServer.call(pid, {:heartbeat, group_id, generation_id, member_id}, @socket_timeout_ms)
  end

  def metadata(pid) do
    GenServer.call(pid, {:metadata}, @socket_timeout_ms)
  end

  def handle_call({:produce, topic, partition, messages}, _from, %{leaders: leaders} = state) do
    {response, new_state} =
      leaders
      |> Map.get(topic)
      |> Map.get(partition)
      |> request_sync(Kafkex.Protocol.Produce, state, topic_data: [[topic: topic, partition: partition, data: messages]])

    {:reply, {:ok, response}, new_state}
  end

  def handle_call({:offsets, topic_partitions, time, max_offsets}, _from, %{leaders: leaders} = state) do
    {responses, new_state} =
      topic_partitions
      |> leaders_for_topic_partitions(leaders)
      |> Enum.map(fn {broker, topic_partitions} -> {broker, Keyword.merge(topic_partitions, time: time, max_offsets: max_offsets)} end)
      |> request_for_brokers(Kafkex.Protocol.Offsets, state)

    response_topic_partitions =
      responses
      |> Enum.flat_map(fn {:ok, %Kafkex.Protocol.Offsets.Response{topic_partitions: topic_partitions}} -> topic_partitions end)

    response = %Kafkex.Protocol.Offsets.Response{topic_partitions: response_topic_partitions}

    {:reply, {:ok, response}, new_state}
  end

  def handle_call({:offset_fetch, group_id, topic_partitions}, _from, state) do
    {{:ok, response}, new_state} =
      group_id
      |> fetch_group_coordinator(state)

    {{:ok, response}, new_state} =
      response
      |> (fn(%Kafkex.Protocol.GroupCoordinator.Response{broker: %Kafkex.Protocol.Broker{node_id: node_id}}) -> node_id end).()
      |> request_sync(Kafkex.Protocol.OffsetFetch, new_state, group_id: group_id, topic_partitions: topic_partitions)

    {:reply, {:ok, response}, new_state}
  end

  def handle_call({:group_coordinator, group_id}, _from, state) do
    {response, new_state} = fetch_group_coordinator(group_id, state)

    {:reply, {:ok, response}, new_state}
  end

  def handle_call({:join_group, topic, group_id, member_id}, _from, state) do
    {{:ok, response}, new_state} =
      group_id
      |> fetch_group_coordinator(state)

    {response, new_state} =
      response
      |> (fn(%Kafkex.Protocol.GroupCoordinator.Response{broker: %Kafkex.Protocol.Broker{node_id: node_id}}) -> node_id end).()
      |> request_sync(Kafkex.Protocol.JoinGroup, new_state, topic: topic, group_id: group_id, member_id: member_id)

    {:reply, {:ok, response}, new_state}
  end

  def handle_call({:sync_group, group_id, generation_id, member_id, group_assignment}, _from, state) do
    {{:ok, response}, new_state} =
      group_id
      |> fetch_group_coordinator(state)

    {response, new_state} =
      response
      |> (fn(%Kafkex.Protocol.GroupCoordinator.Response{broker: %Kafkex.Protocol.Broker{node_id: node_id}}) -> node_id end).()
      |> request_sync(Kafkex.Protocol.SyncGroup, new_state, group_id: group_id, generation_id: generation_id, member_id: member_id, group_assignment: group_assignment)

    {:reply, {:ok, response}, new_state}
  end

  def handle_call({:heartbeat, group_id, generation_id, member_id}, _from, state) do
    {{:ok, response}, new_state} =
      group_id
      |> fetch_group_coordinator(state)

    {response, new_state} =
      response
      |> (fn(%Kafkex.Protocol.GroupCoordinator.Response{broker: %Kafkex.Protocol.Broker{node_id: node_id}}) -> node_id end).()
      |> request_sync(Kafkex.Protocol.Heartbeat, new_state, group_id: group_id, generation_id: generation_id, member_id: member_id)

    {:reply, {:ok, response}, new_state}
  end

  def handle_call({:metadata}, _from, state) do
    {:reply, {:ok, state |> Map.take([:leaders])}, state}
  end

  defp request_sync(broker, module, %{connections: connections, correlation_ids: correlation_ids} = state, options) do
    conn = connections |> Map.get(broker)
    correlation_id = (correlation_ids |> Map.get(broker)) + 1

    :ok = Kafkex.Connection.send(conn, Module.concat(module, Request).build(correlation_id, @client_id, options))
    {^correlation_id, response} = Module.concat(module, Response).parse(Kafkex.Connection.recv(conn, 0, @socket_timeout_ms))

    new_correlation_ids = correlation_ids |> Map.put(broker, correlation_id)

    {response, %{state | correlation_ids: new_correlation_ids}}
  end

  defp request_for_brokers(brokers_with_options, module, state) do
    brokers_with_options
    |> Task.async_stream(fn {broker, options} ->
      {broker, request_sync(broker, module, state, options)}
    end)
    |> Enum.reduce({[], state}, fn {:ok, {broker, {response, _new_state}}}, {responses, prev_state} ->
      next_correlation_id = prev_state[:correlation_ids][broker] + 1
      new_correlation_ids = %{prev_state[:correlation_ids] | broker => next_correlation_id}
      {[response] ++ responses, %{prev_state | correlation_ids: new_correlation_ids}}
    end)
  end

  defp fetch_group_coordinator(group_id, %{group_coordinators: group_coordinators, leaders: leaders} = state) do
    case group_coordinators[group_id] do
      nil ->
        {response, new_state} = leaders
        |> Map.values
        |> Stream.flat_map(&Map.values/1)
        |> Enum.random
        |> request_sync(Kafkex.Protocol.GroupCoordinator, state, group_id: group_id)

        new_group_coordinators = new_state[:group_coordinators] |> Map.put(group_id, response)

        {response, %{new_state | group_coordinators: new_group_coordinators}}
      response ->
        {response, state}
    end
  end

  defp fetch_metadata([]), do: {:error, :no_seed_brokers}
  defp fetch_metadata([seed_broker|rest]) do
    case connect(seed_broker) do
      {:ok, conn} ->
        case Kafkex.Connection.send(conn, Kafkex.Protocol.Metadata.Request.build(0, @client_id)) do
          :ok ->
            {0, {:ok, metadata}} = Kafkex.Protocol.Metadata.Response.parse(Kafkex.Connection.recv(conn, 0, @socket_timeout_ms))
            :ok = Kafkex.Connection.close(conn)
            :ok = GenServer.stop(conn)
            {:ok, metadata}
          {:error, :closed} ->
            fetch_metadata(rest)
        end
    end
  end

  defp build_connections(%Kafkex.Protocol.Metadata.Response{brokers: brokers}) do
    Enum.reduce brokers, %{}, fn(%Kafkex.Protocol.Broker{node_id: node_id, host: host, port: port}, acc) ->
      {:ok, conn} = connect({String.to_charlist(host), port})
      acc |> Map.put(node_id, conn)
    end
  end

  defp build_leaders(%Kafkex.Protocol.Metadata.Response{topic_metadatas: topic_metadatas}) do
    topic_metadatas
    |> Enum.reduce(%{}, fn(topic_metadata, leaders) ->
      topic_leaders = topic_metadata.partition_metadatas |> Enum.reduce(%{}, fn(partition_metadata, partition_leaders) ->
        partition_leaders |> Map.put(partition_metadata.partition_id, partition_metadata.leader)
      end)
      leaders |> Map.put(topic_metadata.topic, topic_leaders)
    end)
  end

  defp initialize_correlation_ids(connections) do
    connections
    |> Map.keys
    |> Enum.reduce(%{}, fn(broker_id, correlation_ids) ->
      correlation_ids |> Map.put(broker_id, 0)
    end)
  end

  defp leaders_for_topic_partitions(topic_partitions, leaders) do
    topic_partitions
    |> Enum.flat_map(fn([topic: topic, partitions: partitions]) -> Enum.map(partitions, fn(partition) -> {topic, partition} end) end)
    |> Enum.group_by(fn {topic, partition} -> leaders[topic][partition] end)
    |> Enum.map(fn {leader, topic_partitions} ->
      topic = topic_partitions |> hd |> elem(0)
      partitions = topic_partitions |> Enum.map(&(elem(&1, 1)))
      {leader, [topic_partitions: [[topic: topic, partitions: partitions]]]}
    end)
  end

  defp connect({host, port}) do
    case Kafkex.Connection.start_link({host, port}) do
      {:ok, _} = success ->
        success
      {:error, reason} = error ->
        Logger.error("[#{__MODULE__}] error connecting to #{inspect(host)}:#{port}: #{reason}")
        error
    end
  end
end
