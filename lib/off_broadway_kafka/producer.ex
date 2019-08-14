defmodule OffBroadwayKafka.Producer do
  use GenStage

  @behaviour Broadway.Acknowledger

  require Logger

  @impl GenStage
  def init(options) do
    {:producer, consumer_state} =
      Kafkex.Consumer.init(
        {options[:seed_brokers], options[:topic], options[:group_id], auto_commit: false}
      )

    {:producer, Map.put(options, :consumer_state, consumer_state)}
  end

  @impl GenStage
  def handle_demand(demand, state) do
    {:noreply, events, consumer_state} =
      Kafkex.Consumer.handle_demand(demand, state[:consumer_state])

    transform_and_return(events, state, consumer_state)
  end

  @impl GenStage
  def handle_info(message, state) do
    {:noreply, events, consumer_state} =
      Kafkex.Consumer.handle_info(message, state[:consumer_state])

    transform_and_return(events, state, consumer_state)
  end

  @impl Broadway.Acknowledger
  def ack(ack_ref, successful, failed) do
    if length(failed) > 0 do
      Logger.warn("not acknowledging #{length(failed)} failed messages: #{failed}",
        limit: :infinity
      )
    end

    kafkex_messages = untransform_messages(successful)

    Kafkex.Consumer.commit(ack_ref, kafkex_messages)
  end

  def transform_and_return(events, state, consumer_state) do
    messages =
      events
      |> Enum.map(&transform(&1, self()))

    {:noreply, messages, %{state | consumer_state: consumer_state}}
  end

  def transform(message, ack_ref) do
    data = message_data(message)
    metadata = message_metadata(message)
    acknowledger = build_acknowledger(ack_ref)
    %Broadway.Message{data: data, metadata: metadata, acknowledger: acknowledger}
  end

  defp message_data(%Kafkex.Protocol.Message{value: value}), do: value

  defp message_metadata(message = %Kafkex.Protocol.Message{}) do
    Map.take(message, [:key, :offset, :partition, :timestamp])
  end

  defp build_acknowledger(ack_ref), do: {__MODULE__, ack_ref, nil}

  defp untransform_messages(messages) do
    messages
    |> Enum.map(fn message ->
      struct(Kafkex.Protocol.Message, Map.put(message.metadata, :value, message.data))
    end)
  end
end
