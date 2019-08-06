defmodule Kafkex.Producer do
  use GenStage

  def start_link({seed_brokers, topic}) do
    start_link({seed_brokers, topic, %{}})
  end

  def start_link({seed_brokers, topic, options}) do
    if options[:name] do
      GenStage.start_link(__MODULE__, {seed_brokers, topic}, name: options[:name])
    else
      GenStage.start_link(__MODULE__, {seed_brokers, topic})
    end
  end

  def init({seed_brokers, topic}) do
    result =
      with {:ok, client} <- Kafkex.Client.start_link(seed_brokers),
           {:ok, %{leaders: leaders}} <- Kafkex.Client.metadata(client),
           {:ok, num_partitions} <- extract_partitions(leaders, topic) do
        {:consumer,
         %{client: client, topic: topic, num_partitions: num_partitions, last_partition: -1}}
      end

    case result do
      {:error, reason} -> {:stop, reason}
      consumer -> consumer
    end
  end

  def handle_events(
        events,
        _from,
        %{
          client: client,
          topic: topic,
          num_partitions: num_partitions,
          last_partition: last_partition
        } = state
      ) do
    partition = next_partition(num_partitions, last_partition)
    {:ok, _} = Kafkex.Client.produce(client, topic, partition, events)
    {:noreply, [], %{state | last_partition: partition}}
  end

  def handle_info({{_pid, _subscription_tag}, {:producer, :done}}, state),
    do: {:noreply, [], state}

  defp next_partition(_, -1), do: 0
  defp next_partition(num_partitions, last_partition), do: rem(last_partition + 1, num_partitions)

  def extract_partitions(leaders, topic) do
    num_partitions = (Map.get(leaders, topic) || %{}) |> Map.size

    if num_partitions > 0 do
      {:ok, num_partitions}
    else
      {:error, "No partitions found for topic " <> topic}
    end
  end
end
