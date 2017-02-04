defmodule Kafkex.Producer do
  use GenStage

  def start_link(topic) do
    GenStage.start_link(__MODULE__, topic)
  end

  def init(topic) do
    {:ok, %{leaders: leaders}} = Kafkex.Client.metadata
    num_partitions = leaders |> Map.get(topic) |> Map.size
    {:consumer, %{topic: topic, num_partitions: num_partitions, last_partition: -1}}
  end

  def handle_events(events, _from, %{topic: topic, num_partitions: num_partitions, last_partition: last_partition} = state) do
    partition = next_partition(num_partitions, last_partition)
    {:ok, _} = Kafkex.Client.produce(topic, partition, events)
    {:noreply, [], %{state | last_partition: partition}}
  end

  def handle_info({{_pid, _subscription_tag}, {:producer, :done}}, state), do: {:noreply, [], state}

  defp next_partition(_, -1), do: 0
  defp next_partition(num_partitions, last_partition), do: rem(last_partition + 1, num_partitions)
end
