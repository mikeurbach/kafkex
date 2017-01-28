defmodule Kafkex.Client do
  use GenServer

  require Logger

  @client_id "kafkex"
  @socket_timeout_ms 10_000

  def start_link(seed_brokers) do
    GenServer.start_link(__MODULE__, seed_brokers)
  end

  def init(seed_brokers) do
    case fetch_metadata(seed_brokers) do
      {:ok, metadata} ->
        connections = build_connections(metadata)
        leaders = build_leaders(metadata)

        {:ok, %{metadata: metadata, connections: connections, leaders: leaders}}
      {:error, _} = error ->
        {:stop, error}
    end
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
