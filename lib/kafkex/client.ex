defmodule Kafkex.Client do
  use GenServer

  require Logger

  @client_id "kafkex"
  @socket_timeout_ms 10_000

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

  def produce(topic, partition, message) when is_binary(message) do
    GenServer.call(__MODULE__, {:produce, topic, partition, message})
  end

  def handle_call({:produce, topic, partition, message}, _from, %{connections: connections, leaders: leaders, correlation_ids: correlation_ids} = state) do
    leader = leaders |> Map.get(topic) |> Map.get(partition)
    conn = connections |> Map.get(leader)
    correlation_id = (correlation_ids |> Map.get(leader)) + 1

    :ok = Kafkex.Connection.send(conn, Kafkex.Protocol.Produce.Request.build(correlation_id, @client_id, topic_data: [[topic: topic, partition: partition, data: [message]]]))
    {^correlation_id, response} = Kafkex.Protocol.Produce.Response.parse(Kafkex.Connection.recv(conn, 0, @socket_timeout_ms))

    new_correlation_ids = correlation_ids |> Map.put(leader, correlation_id)

    {:reply, {:ok, response}, state |> Map.put(:correlation_ids, new_correlation_ids)}
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
    Enum.reduce brokers, %{}, fn(%Kafkex.Protocol.Metadata.Broker{node_id: node_id, host: host, port: port}, acc) ->
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
