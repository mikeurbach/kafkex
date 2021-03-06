defmodule Kafkex.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kafkex,
      version: "0.1.0",
      elixir: "~> 1.3",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:connection, :logger], mod: {Kafkex, []}]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {:broadway, "~> 0.4"},
      {:connection, "~> 1.0.4"},
      {:flow, "~> 0.14"},
      {:gen_stage, "~> 0.14"}
    ]
  end
end
