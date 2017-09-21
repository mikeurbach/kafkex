defmodule Kafkex.Protocol.Fetch do
  import Kafkex.Protocol

  defmodule Partition do
    @default_max_bytes 1048576

    defstruct partition: -1, error_code: 0, high_watermark: -1, messages: []

    def build([partition: partition, offset: offset] = options) do
      max_bytes = options |> Keyword.get(:max_bytes, @default_max_bytes)
      << partition :: 32, offset :: 64, max_bytes :: 32 >>
    end

    def parse(<< partition :: 32, partition_error_code :: 16, high_watermark :: 64-signed, message_size :: 32, rest :: binary >>) do
      {messages, new_rest} = Kafkex.Protocol.Message.parse(message_size, rest, [])
      {%Kafkex.Protocol.Fetch.Partition{partition: partition, error_code: error_code(partition_error_code), high_watermark: high_watermark, messages: Enum.reverse(messages)}, new_rest}
    end
  end

  defmodule TopicPartitions do
    defstruct topic: "", partitions: []

    def build([topic: topic, partitions: partitions]) do
      partitions_list = partitions |> build_list(&Kafkex.Protocol.Fetch.Partition.build/1)
      [build_item(topic), partitions_list]
    end

    def parse(<< topic_length :: 16, topic :: size(topic_length)-binary, partitions_length :: 32, rest :: binary >>) do
      {partitions, new_rest} = parse_list(partitions_length, rest, &Kafkex.Protocol.Fetch.Partition.parse/1)
      {%Kafkex.Protocol.Fetch.TopicPartitions{topic: topic, partitions: partitions}, new_rest}
    end
  end

  defmodule Request do
    @api_key 1
    @api_version 2

    @default_max_wait_time 1000
    @default_min_bytes 1
    @replica_id -1

    defstruct max_wait_time: -1, min_bytes: -1, topic_partitions: []

    def build(correlation_id, client_id, [topic_partitions: topic_partitions] = options) do
      max_wait_time = options |> Keyword.get(:max_wait_time, @default_max_wait_time)
      min_bytes = options |> Keyword.get(:min_bytes, @default_min_bytes)
      options = << @replica_id :: 32, max_wait_time :: 32, min_bytes :: 32 >>
      topic_partitions_list = topic_partitions |> build_list(&Kafkex.Protocol.Fetch.TopicPartitions.build/1)
      [build_headers(@api_key, @api_version, correlation_id, client_id), options, topic_partitions_list]
    end
  end

  defmodule Response do
    defstruct topic_responses: [], throttle_time_ms: 32

    def parse({:ok, << correlation_id :: 32, throttle_time_ms :: 32, topic_partitions_length :: 32, rest :: binary >>}) do
      {topic_responses, <<>>} = parse_list(topic_partitions_length, rest, &Kafkex.Protocol.Fetch.TopicPartitions.parse/1)
      {correlation_id, {:ok, %Kafkex.Protocol.Fetch.Response{topic_responses: topic_responses, throttle_time_ms: throttle_time_ms}}}
    end
  end
end
