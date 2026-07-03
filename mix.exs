defmodule Docket.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :docket,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:crypto, :logger]
    ]
  end

  defp deps do
    []
  end
end
