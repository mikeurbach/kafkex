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

