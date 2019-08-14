defmodule OffBroadway.Kafka.Transformer do
  @behaviour Broadway.Acknowledger

  require Logger

  def transform(message, [producer_name]) do
    data = message_data(message)
    metadata = message_metadata(message)
    acknowledger = build_acknowledger(producer_name)
    %Broadway.Message{data: data, metadata: metadata, acknowledger: acknowledger}
  end

  @impl Broadway.Acknowledger
  def ack(ack_ref, successful, failed) do
    if length(failed) > 0 do
      Logger.warn("not acknowledging #{length(failed)} failed messages: #{failed}",
        limit: :infinity
      )
    end

    kafkex_messages = unformat_messages(successful)

    Kafkex.Consumer.commit(ack_ref, kafkex_messages)
  end

  defp message_data(%Kafkex.Protocol.Message{value: value}), do: value

  defp message_metadata(message = %Kafkex.Protocol.Message{}) do
    Map.take(message, [:key, :offset, :partition, :timestamp])
  end

  defp build_acknowledger(ack_ref), do: {__MODULE__, ack_ref, nil}

  defp unformat_messages(messages) do
    messages
    |> Enum.map(fn message ->
      struct(Kafkex.Protocol.Message, Map.put(message.metadata, :value, message.data))
    end)
  end
end
