defmodule Kafkex.Protocol.Produce do
  import Kafkex.Protocol

  defmodule Request do
    @api_key 0
    @api_version 0
    @default_acks 1
    @default_timeout 10_000
    @magic_byte 1
    @attributes 0

    defstruct acks: 0, timeout: 0, topic_data: []

    def build(correlation_id, client_id, [topic_data: topic_data] = options) do
      acks = options |> Keyword.get(:acks, @default_acks)
      timeout = options |> Keyword.get(:timeout, @default_timeout)

      build_headers(@api_key, @api_version, correlation_id, client_id) <> << acks :: 16 >> <> << timeout :: 32 >> <> << length(topic_data) :: 32 >> <> build_data(topic_data)
    end

    defp build_data([]), do: <<>>
    defp build_data([[topic: topic, partition: partition, data: data]|rest]) do
      messages = build_messages(data)
      build_item(topic) <> << 1 :: 32 >> <> << partition :: 32 >> <> << byte_size(messages) :: 32 >> <> messages <> build_data(rest)
    end

    defp build_messages([]), do: <<>>
    defp build_messages([{key,value}|rest]) do
      core = << @magic_byte :: 8 >> <> << @attributes :: 8 >> <> << timestamp :: 64 >> <> build_item(key, 32) <> build_item(value, 32)
      crc = :erlang.crc32(core)
      message = << crc :: 32 >> <> core
      << 0 :: 64 >> <> << byte_size(message) :: 32 >> <> message <> build_messages(rest)
    end
    defp build_messages([value|rest]), do: build_messages([{nil,value}|rest])

    defp timestamp, do: DateTime.utc_now |> DateTime.to_unix(:milliseconds)
  end

  defmodule Response do
    defstruct responses: []

    def parse({:ok, << correlation_id :: 32, responses_length :: 32, rest :: binary>>}) do
      {responses, <<>>} = parse_list(responses_length, rest, &Kafkex.Protocol.Produce.TopicResponse.build/1)
      {correlation_id, %Response{responses: responses}}
    end
  end

  defmodule TopicResponse do
    defstruct topic: "", partition_responses: []

    def build(<< topic_length :: 16, topic :: size(topic_length)-binary, partition_responses_length :: 32, rest :: binary >>) do
      {partition_responses, rest} = parse_list(partition_responses_length, rest, &Kafkex.Protocol.Produce.PartitionResponse.build/1)
      {%TopicResponse{topic: topic, partition_responses: partition_responses}, rest}
    end
  end

  defmodule PartitionResponse do
    defstruct partition: 0, error_code: 0, base_offset: 0

    def build(<< partition :: 32, partition_error_code :: 16, base_offset :: 64, rest :: binary >>) do
      {%PartitionResponse{partition: partition, error_code: error_code(partition_error_code), base_offset: base_offset}, rest}
    end
  end
end
