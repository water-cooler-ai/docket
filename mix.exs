defmodule Docket.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :docket,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  def application do
    [
      extra_applications: [:crypto, :logger]
    ]
  end

  defp deps do
    []
  end
end
