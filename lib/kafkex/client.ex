defmodule Kafkex.Client do
  use GenServer

  require Logger

  @client_id "kafkex"
  @socket_timeout_ms 30_000

  def start_link(seed_brokers) do
    GenServer.start_link(__MODULE__, seed_brokers, name: __MODULE__)
  end

  def init(seed_brokers) do
    case fetch_metadata(seed_brokers) do
      {:ok, metadata} ->
        connections = build_connections(metadata)
        leaders = build_leaders(metadata)
        correlation_ids = initialize_correlation_ids(connections)

        {:ok, %{metadata: metadata, connections: connections, leaders: leaders, correlation_ids: correlation_ids}}
      {:error, _} = error ->
        {:stop, error}
    end
  end

  def produce(topic, partition, messages) do
    GenServer.call(__MODULE__, {:produce, topic, partition, messages})
  end

  def group_coordinator(group_id) do
    GenServer.call(__MODULE__, {:group_coordinator, group_id})
  end

  def join_group(group_id, topic) do
    GenServer.call(__MODULE__, {:join_group, group_id, topic})
  end

  def sync_group(group_id, generation_id, member_id, group_assignment) do
    GenServer.call(__MODULE__, {:sync_group, group_id, generation_id, member_id, group_assignment})
  end

  def metadata() do
    GenServer.call(__MODULE__, {:metadata})
  end

  def handle_call({:produce, topic, partition, messages}, _from, %{leaders: leaders} = state) do
    {response, new_state} =
      leaders
      |> Map.get(topic)
      |> Map.get(partition)
      |> request_sync(Kafkex.Protocol.Produce, state, topic_data: [[topic: topic, partition: partition, data: messages]])

    {:reply, {:ok, response}, new_state}
  end

  def handle_call({:group_coordinator, group_id}, _from, state) do
    {response, new_state} = fetch_group_coordinator(group_id, state)

    {:reply, {:ok, response}, new_state}
  end

  def handle_call({:join_group, group_id, topic}, _from, state) do
    {response, new_state} =
      group_id
      |> fetch_group_coordinator(state)

    {response, new_state} =
      response
      |> (fn(%Kafkex.Protocol.GroupCoordinator.Response{broker: %Kafkex.Protocol.Broker{node_id: node_id}}) -> node_id end).()
      |> request_sync(Kafkex.Protocol.JoinGroup, new_state, group_id: group_id, topic: topic)

    {:reply, {:ok, response}, new_state}
  end

  def handle_call({:sync_group, group_id, generation_id, member_id, group_assignment}, _from, state) do
    {response, new_state} =
      group_id
      |> fetch_group_coordinator(state)

    {response, new_state} =
      response
      |> (fn(%Kafkex.Protocol.GroupCoordinator.Response{broker: %Kafkex.Protocol.Broker{node_id: node_id}}) -> node_id end).()
      |> request_sync(Kafkex.Protocol.SyncGroup, new_state, group_id: group_id, generation_id: generation_id, member_id: member_id, group_assignment: group_assignment)

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

  # TODO: this could be cached
  defp fetch_group_coordinator(group_id, %{leaders: leaders} = state) do
    leaders
    |> Map.values
    |> Stream.flat_map(&Map.values/1)
    |> Enum.random
    |> request_sync(Kafkex.Protocol.GroupCoordinator, state, group_id: group_id)
  end

  defp fetch_metadata([]), do: {:error, :no_seed_brokers}
  defp fetch_metadata([seed_broker|rest]) do
    case connect(seed_broker) do
      {:ok, conn} ->
        case Kafkex.Connection.send(conn, Kafkex.Protocol.Metadata.Request.build(0, @client_id)) do
          :ok ->
            {0, metadata} = Kafkex.Protocol.Metadata.Response.parse(Kafkex.Connection.recv(conn, 0, @socket_timeout_ms))
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
      {:ok, conn} = connect({String.to_char_list(host), port})
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
