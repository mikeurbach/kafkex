defmodule Kafkex do
  use Application

  # See http://elixir-lang.org/docs/stable/elixir/Application.html
  # for more information on OTP Applications
  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    seed_brokers = Application.fetch_env!(:kafkex, :seed_brokers)

    # Define workers and child supervisors to be supervised
    children = [
      # Starts a worker by calling: Kafkex.Worker.start_link(arg1, arg2, arg3)
      # worker(Kafkex.Client, [seed_brokers])
    ]

    # See http://elixir-lang.org/docs/stable/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Kafkex.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
