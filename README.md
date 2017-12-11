# Kafkex

## Background

https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `kafkex` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:kafkex, git: "https://github.com/mikeurbach/kafkex.git", tag: "0.1"}]
    end
    ```

  2. Ensure `kafkex` is started before your application:

    ```elixir
    def application do
      [applications: [:kafkex]]
    end
    ```

## Examples

```
iex(1)> {:ok, producer} = Kafkex.Producer.start_link({[{'localhost', 9092}], "foo"})
{:ok, #PID<0.188.0>}
iex(2)> Stream.repeatedly(fn() -> {"key", "value"} end) |> Flow.from_enumerable |> Flow.into_stages([producer])
{:ok, #PID<0.194.0>}
iex(3)> {:ok, consumer} = Kafkex.Consumer.start_link({[{'localhost', 9092}], "foo", "group"})
{:ok, #PID<0.149.0>}
iex(4)> Flow.from_stage(consumer) |> Enum.take(3)
[%Kafkex.Protocol.Message{key: "key", offset: 155227127,
  timestamp: 1513012816377, value: "value"},
 %Kafkex.Protocol.Message{key: "key", offset: 155227128,
  timestamp: 1513012816377, value: "value"},
 %Kafkex.Protocol.Message{key: "key", offset: 155227129,
  timestamp: 1513012816377, value: "value"}]
```
