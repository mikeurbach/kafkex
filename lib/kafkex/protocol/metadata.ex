defmodule Kafkex.Protocol.Metadata do
  import Kafkex.Protocol

  defmodule Request do
    @api_key 3
    @api_version 0

    defstruct topics: nil

    def build(correlation_id, client_id, options \\ []) do
      case options |> Keyword.get(:topics) do
        nil ->
          build_headers(@api_key, @api_version, correlation_id, client_id) <> <<0::32>>

        topics when is_list(topics) ->
          build_headers(@api_key, @api_version, correlation_id, client_id) <>
            build_primitive_list(topics)
      end
    end
  end

  defmodule Response do
    defstruct brokers: [], topic_metadatas: []

    def parse({:ok, <<correlation_id::32, brokers_length::32, rest::binary>>}) do
      {brokers, <<topic_metadatas_length::32, rest::binary>>} =
        parse_list(brokers_length, rest, &Kafkex.Protocol.Broker.build/1)

      {topic_metadatas, <<>>} =
        parse_list(topic_metadatas_length, rest, &Kafkex.Protocol.Metadata.TopicMetadata.build/1)

      {correlation_id, {:ok, %Response{brokers: brokers, topic_metadatas: topic_metadatas}}}
    end
  end

  defmodule TopicMetadata do
    defstruct error_code: 0, topic: "", partition_metadatas: []

    def build(
          <<topic_error_code::16, topic_length::16, topic::size(topic_length)-binary,
            partitions_length::32, rest::binary>>
        ) do
      {partition_metadatas, rest} =
        parse_list(partitions_length, rest, &Kafkex.Protocol.Metadata.PartitionMetadata.build/1)

      {%TopicMetadata{
         error_code: error_code(topic_error_code),
         topic: topic,
         partition_metadatas: partition_metadatas
       }, rest}
    end
  end

  defmodule PartitionMetadata do
    defstruct error_code: 0, partition_id: 0, leader: 0, replicas: [], isr: []

    def build(
          <<partition_error_code::16, partition_id::32, leader::32, replicas_length::32,
            rest::binary>>
        ) do
      {replicas, <<isr_length::32, rest::binary>>} =
        parse_list(replicas_length, rest, fn <<replica::32, rest::binary>> -> {replica, rest} end)

      {isr, rest} = parse_list(isr_length, rest, fn <<isr::32, rest::binary>> -> {isr, rest} end)

      {%PartitionMetadata{
         error_code: error_code(partition_error_code),
         partition_id: partition_id,
         leader: leader,
         replicas: replicas,
         isr: isr
       }, rest}
    end
  end
end
