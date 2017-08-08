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
iex(1)> {:ok, producer1} = Kafkex.Producer.start_link({[{'localhost', 9092}], "foo"})
{:ok, #PID<0.176.0>}
iex(2)> {:ok, producer2} = Kafkex.Producer.start_link({[{'localhost', 9092}], "foo"})
{:ok, #PID<0.182.0>}
iex(3)> {:ok, producer3} = Kafkex.Producer.start_link({[{'localhost', 9092}], "foo"})
{:ok, #PID<0.188.0>}
iex(4)> ["foo", "bar", "baz"] |> Flow.from_enumerable |> Flow.into_stages([producer1, producer2, producer3])
{:ok, #PID<0.194.0>}
```

```
iex(1)> {:ok, consumer1} = Kafkex.Consumer.start_link({[{'localhost', 9092}], "foo", "group"})
{:ok, #PID<0.149.0>}
iex(2)> {:ok, consumer2} = Kafkex.Consumer.start_link({[{'localhost', 9092}], "foo", "group"})
{:ok, #PID<0.158.0>}
iex(3)> {:ok, consumer3} = Kafkex.Consumer.start_link({[{'localhost', 9092}], "foo", "group"})
{:ok, #PID<0.166.0>}
```
