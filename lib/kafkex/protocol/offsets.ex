defmodule Kafkex.Protocol.Offsets do
  import Kafkex.Protocol

  defmodule Partition do
    defstruct partition: -1, error_code: 0, offsets: []

    def build({time, max_offsets, partition}), do: << partition :: 32, time :: 64, max_offsets :: 32 >>
    def parse(<< partition :: 32, partition_error_code :: 16, offsets_length :: 32, rest :: binary >>) do
      {offsets, new_rest} = parse_list(offsets_length, rest, fn(<< offset :: 64, rest :: binary >>) -> {offset, rest} end)
      {%Kafkex.Protocol.Offsets.Partition{partition: partition, error_code: partition_error_code, offsets: offsets}, new_rest}
    end
  end

  defmodule TopicPartitions do
    defstruct topic: "", partitions: []

    def build({time, max_offsets, [topic: topic, partitions: partitions]}) do
      partitions_list = partitions
      |> Enum.map(&({time, max_offsets, &1}))
      |> build_list(&Kafkex.Protocol.Offsets.Partition.build/1)

      [build_item(topic), partitions_list]
    end

    def parse(<< topic_length :: 16, topic :: size(topic_length)-binary, partitions_length :: 32, rest :: binary >>) do
      {partitions, new_rest} = parse_list(partitions_length, rest, &Kafkex.Protocol.Offsets.Partition.parse/1)
      {%Kafkex.Protocol.Offsets.TopicPartitions{topic: topic, partitions: partitions}, new_rest}
    end
  end

  defmodule Request do
    @api_key 2
    @api_version 0
    @default_replica_id -1

    defstruct replica_id: -1, topics_partitions: []

    def build(correlation_id, client_id, [topic_partitions: topic_partitions, time: time, max_offsets: max_offsets]) do
      topic_partitions_list = topic_partitions
      |> Enum.map(&({time, max_offsets, &1}))
      |> build_list(&Kafkex.Protocol.Offsets.TopicPartitions.build/1)

      [build_headers(@api_key, @api_version, correlation_id, client_id), << @default_replica_id :: 32 >>, topic_partitions_list]
    end
  end

  defmodule Response do
    defstruct topic_partitions: []

    def parse({:ok, << correlation_id :: 32, topic_partitions_length :: 32, rest :: binary >>}) do
      {topic_partitions, <<>>} = parse_list(topic_partitions_length, rest, &Kafkex.Protocol.Offsets.TopicPartitions.parse/1)
      {correlation_id, {:ok, %Kafkex.Protocol.Offsets.Response{topic_partitions: topic_partitions}}}
    end
  end
end
