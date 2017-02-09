defmodule Kafkex.Protocol.GroupCoordinator do
  import Kafkex.Protocol

  defmodule Request do
    @api_key 10
    @api_version 0

    defstruct group_id: ""

    def build(correlation_id, client_id, [group_id: group_id]) do
      build_headers(@api_key, @api_version, correlation_id, client_id) <> build_item(group_id)
    end
  end

  defmodule Response do
    defstruct broker: []

    def parse({:ok, << correlation_id :: 32, group_error_code :: 16, coordinator :: binary >>}) do
      :NONE = error_code(group_error_code)
      {broker, <<>>} = Kafkex.Protocol.Broker.build(coordinator)
      {correlation_id, %Response{broker: broker}}
    end
  end
end
