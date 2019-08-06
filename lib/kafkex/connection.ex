defmodule Kafkex.Connection do
  use Connection

  require Logger

  @connect_options [:binary, active: false, packet: 4]
  @connect_timeout 30_000
  @max_tries 5

  def start_link({host, port}) do
    Connection.start_link(__MODULE__, {host, port})
  end

  def init({host, port}) do
    {:connect, :init, %{host: host, port: port, socket: nil, tries: 0}}
  end

  def send(conn, data), do: Connection.call(conn, {:send, data}, @connect_timeout)

  def recv(conn, bytes, timeout),
    do: Connection.call(conn, {:recv, bytes, timeout}, @connect_timeout)

  def close(conn), do: Connection.call(conn, :close)

  def connect(_, %{host: host, port: port, socket: nil, tries: tries} = state) do
    case :gen_tcp.connect(host, port, @connect_options, @connect_timeout) do
      {:ok, socket} ->
        {:ok, %{state | socket: socket, tries: 0}}

      {:error, _} = error ->
        case tries do
          n when n < @max_tries ->
            Logger.error("[#{__MODULE__}] error: #{inspect(error)}, backing off")
            {:backoff, backoff_timeout(tries), %{state | tries: tries + 1}}

          _ ->
            {:stop, error, state}
        end
    end
  end

  def disconnect(info, %{socket: socket} = state) do
    case info do
      {:close, from} ->
        Connection.reply(from, :gen_tcp.close(socket))

      {:error, :closed} ->
        Logger.error("[#{__MODULE__}] connection closed")

      {:error, reason} ->
        Logger.error("[#{__MODULE__}] connection error: #{:inet.format_error(reason)}")
    end

    {:connect, :reconnect, %{state | socket: nil}}
  end

  def handle_call(_, _, %{socket: nil} = state), do: {:reply, {:error, :closed}, state}

  def handle_call({:send, data}, _, %{socket: socket} = state) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:reply, :ok, state}

      {:error, _} = error ->
        {:disconnect, error, error, state}
    end
  end

  def handle_call({:recv, bytes, timeout}, _, %{socket: socket} = state) do
    case :gen_tcp.recv(socket, bytes, timeout) do
      {:ok, _} = ok ->
        {:reply, ok, state}

      {:error, :timeout} = timeout ->
        {:reply, timeout, state}

      {:error, _} = error ->
        {:disconnect, error, error, state}
    end
  end

  def handle_call(:close, from, state) do
    {:disconnect, {:close, from}, state}
  end

  defp backoff_timeout(tries), do: round(:math.pow(2, tries) + :rand.uniform()) * 1000
end
