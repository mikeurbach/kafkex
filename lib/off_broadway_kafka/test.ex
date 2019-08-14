defmodule OffBroadway.Kafka.Test do
  use Broadway

  require Logger

  def start_link(_options) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        kafka: [
          module:
            {Kafkex.Consumer, {[{'localhost', 9092}], "foo", "group", %{auto_commit: false}}},
          transformer: {OffBroadway.Kafka.Transformer, :transform, [__MODULE__]}
        ]
      ],
      processors: [
        default: []
      ],
      batchers: [
        test: [
          batch_size: 4,
          batch_timeout: 1000
        ]
      ]
    )
  end

  def handle_message(name, message, context) do
    Logger.info(
      "processor #{name} handling message #{inspect(message)} with context #{inspect(context)}"
    )

    Broadway.Message.put_batcher(message, :test)
  end

  def handle_batch(:test, messages, batch_info, context) do
    Logger.info(
      "batcher :test handling #{length(messages)} messages #{inspect(messages)} with batch info #{
        inspect(batch_info)
      } and context #{inspect(context)}"
    )

    messages
  end
end
