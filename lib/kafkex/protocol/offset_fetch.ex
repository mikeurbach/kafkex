defmodule Kafkex.Protocol.OffsetFetch do
  import Kafkex.Protocol

  defmodule Partition do
    defstruct partition: -1, offset: -1, metadata: "", error_code: 0

    def build(partition), do: <<partition::32>>

    def parse(
          <<partition::32, offset::64-signed, metadata_length::16,
            metadata::size(metadata_length)-binary, partition_error_code::16, rest::binary>>
        ) do
      {%Kafkex.Protocol.OffsetFetch.Partition{
         partition: partition,
         offset: offset,
         metadata: metadata,
         error_code: error_code(partition_error_code)
       }, rest}
    end
  end

  defmodule TopicPartitions do
    defstruct topic: "", partitions: []

    def build(topic: topic, partitions: partitions) do
      partitions_list = partitions |> build_list(&Kafkex.Protocol.OffsetFetch.Partition.build/1)
      [build_item(topic), partitions_list]
    end

    def parse(
          <<topic_length::16, topic::size(topic_length)-binary, partitions_length::32,
            rest::binary>>
        ) do
      {partitions, new_rest} =
        parse_list(partitions_length, rest, &Kafkex.Protocol.OffsetFetch.Partition.parse/1)

      {%Kafkex.Protocol.OffsetFetch.TopicPartitions{topic: topic, partitions: partitions},
       new_rest}
    end
  end

  defmodule Request do
    @api_key 9
    @api_version 1

    defstruct group_id: "", topics_partitions: []

    def build(correlation_id, client_id, group_id: group_id, topic_partitions: topic_partitions) do
      topic_partitions_list =
        topic_partitions |> build_list(&Kafkex.Protocol.OffsetFetch.TopicPartitions.build/1)

      [
        build_headers(@api_key, @api_version, correlation_id, client_id),
        build_item(group_id),
        topic_partitions_list
      ]
    end
  end

  defmodule Response do
    defstruct topic_partitions: []

    def parse({:ok, <<correlation_id::32, topic_partitions_length::32, rest::binary>>}) do
      {topic_partitions, <<>>} =
        parse_list(
          topic_partitions_length,
          rest,
          &Kafkex.Protocol.OffsetFetch.TopicPartitions.parse/1
        )

      {correlation_id,
       {:ok, %Kafkex.Protocol.OffsetFetch.Response{topic_partitions: topic_partitions}}}
    end
  end
end
