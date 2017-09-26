defmodule Kafkex.Protocol.OffsetCommit do
  import Kafkex.Protocol

  defmodule Partition do
    defstruct partition: -1, error_code: 0

    def build([partition: partition, offset: offset, metadata: metadata]) do
      [<< partition :: 32 >>, << offset :: 64 >>, build_item(metadata)]
    end

    def parse(<< partition :: 32, partition_error_code :: 16, rest :: binary >>) do
      {%Kafkex.Protocol.OffsetCommit.Partition{partition: partition, error_code: error_code(partition_error_code)}, rest}
    end
  end

  defmodule TopicPartitions do
    defstruct topic: "", partitions: []

    def build([topic: topic, partitions: partitions]) do
      partitions_list = partitions |> build_list(&Kafkex.Protocol.OffsetCommit.Partition.build/1)
      [build_item(topic), partitions_list]
    end

    def parse(<< topic_length :: 16, topic :: size(topic_length)-binary, partitions_length :: 32, rest :: binary >>) do
      {partitions, new_rest} = parse_list(partitions_length, rest, &Kafkex.Protocol.OffsetCommit.Partition.parse/1)
      {%Kafkex.Protocol.OffsetCommit.TopicPartitions{topic: topic, partitions: partitions}, new_rest}
    end
  end

  defmodule Request do
    @api_key 8
    @api_version 2

    defstruct group_id: "", generation_id: "", member_id: "", retention_time_ms: -1, topics_partitions: []

    def build(correlation_id, client_id, [group_id: group_id, generation_id: generation_id, member_id: member_id, retention_time_ms: retention_time_ms, topic_partitions: topic_partitions]) do
      topic_partitions_list = topic_partitions |> build_list(&Kafkex.Protocol.OffsetCommit.TopicPartitions.build/1)
      [build_headers(@api_key, @api_version, correlation_id, client_id), build_item(group_id), << generation_id :: 32 >>, build_item(member_id), << retention_time_ms :: 64-signed >>, topic_partitions_list]
    end
  end

  defmodule Response do
    defstruct topic_partitions: []

    def parse({:ok, << correlation_id :: 32, topic_partitions_length :: 32, rest :: binary >>}) do
      {topic_partitions, <<>>} = parse_list(topic_partitions_length, rest, &Kafkex.Protocol.OffsetCommit.TopicPartitions.parse/1)
      {correlation_id, {:ok, %Kafkex.Protocol.OffsetCommit.Response{topic_partitions: topic_partitions}}}
    end
  end
end
