defmodule Kafkex.Protocol do
  def error_code(-1), do: :UNKNOWN
  def error_code(0), do: :NONE
  def error_code(1), do: :OFFSET_OUT_OF_RANGE
  def error_code(2), do: :CORRUPT_MESSAGE
  def error_code(3), do: :UNKNOWN_TOPIC_OR_PARTITION
  def error_code(4), do: :INVALID_FETCH_SIZE
  def error_code(5), do: :LEADER_NOT_AVAILABLE
  def error_code(6), do: :NOT_LEADER_FOR_PARTITION
  def error_code(7), do: :REQUEST_TIMED_OUT
  def error_code(8), do: :BROKER_NOT_AVAILABLE
  def error_code(9), do: :REPLICA_NOT_AVAILABLE
  def error_code(10), do: :MESSAGE_TOO_LARGE
  def error_code(11), do: :STALE_CONTROLLER_EPOCH
  def error_code(12), do: :OFFSET_METADATA_TOO_LARGE
  def error_code(13), do: :NETWORK_EXCEPTION
  def error_code(14), do: :GROUP_LOAD_IN_PROGRESS
  def error_code(15), do: :GROUP_COORDINATOR_NOT_AVAILABLE
  def error_code(16), do: :NOT_COORDINATOR_FOR_GROUP
  def error_code(17), do: :INVALID_TOPIC_EXCEPTION
  def error_code(18), do: :RECORD_LIST_TOO_LARGE
  def error_code(19), do: :NOT_ENOUGH_REPLICAS
  def error_code(20), do: :NOT_ENOUGH_REPLICAS_AFTER_APPEND
  def error_code(21), do: :INVALID_REQUIRED_ACKS
  def error_code(22), do: :ILLEGAL_GENERATION
  def error_code(23), do: :INCONSISTENT_GROUP_PROTOCOL
  def error_code(24), do: :INVALID_GROUP_ID
  def error_code(25), do: :UNKNOWN_MEMBER_ID
  def error_code(26), do: :INVALID_SESSION_TIMEOUT
  def error_code(27), do: :REBALANCE_IN_PROGRESS
  def error_code(28), do: :INVALID_COMMIT_OFFSET_SIZE
  def error_code(29), do: :TOPIC_AUTHORIZATION_FAILED
  def error_code(30), do: :GROUP_AUTHORIZATION_FAILED
  def error_code(31), do: :CLUSTER_AUTHORIZATION_FAILED
  def error_code(32), do: :INVALID_TIMESTAMP
  def error_code(33), do: :UNSUPPORTED_SASL_MECHANISM
  def error_code(34), do: :ILLEGAL_SASL_STATE
  def error_code(35), do: :UNSUPPORTED_VERSION
  def error_code(36), do: :TOPIC_ALREADY_EXISTS
  def error_code(37), do: :INVALID_PARTITIONS
  def error_code(38), do: :INVALID_REPLICATION_FACTOR
  def error_code(39), do: :INVALID_REPLICA_ASSIGNMENT
  def error_code(40), do: :INVALID_CONFIG
  def error_code(41), do: :NOT_CONTROLLER
  def error_code(42), do: :INVALID_REQUEST
  def error_code(43), do: :UNSUPPORTED_FOR_MESSAGE_FORMAT

  def build_headers(api_key, api_version, correlation_id, client_id) do
    << api_key :: 16, api_version :: 16, correlation_id :: 32, byte_size(client_id) :: 16, client_id :: binary >>
  end

  def build_list(items, builder), do: build_list(0, items, builder, [])
  def build_list(length, [], _, rest), do: [<< length :: 32, >>, rest]
  def build_list(length, [item|items], builder, rest), do: build_list(length + 1, items, builder, [builder.(item), rest])

  def build_primitive_list(items) when is_list(items) do
    << length(items) :: 32, build_items(items) :: binary >>
  end

  def build_items([]), do: []
  def build_items([item|items]), do: [build_item(item), build_items(items)]

  def build_item(item, size_bits \\ 16)
  def build_item(item, size_bits) when is_nil(item), do: << -1 :: size(size_bits) >>
  def build_item(item, size_bits) when is_binary(item), do: << byte_size(item) :: size(size_bits), item :: binary >>

  def parse_list(length, rest, parser), do: parse_list(length, rest, parser, [])
  def parse_list(0, rest, _, items), do: {items, rest}
  def parse_list(length, rest, parser, items) do
    {item, new_rest} = parser.(rest)
    parse_list(length - 1, new_rest, parser, [item | items])
  end
end
