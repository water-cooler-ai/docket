defmodule Docket.MixProject do
  use Mix.Project

  @version "0.1.0-dev"
  @source_url "https://github.com/water-cooler-ai/docket"

  # Set by the core-only CI leg to build and test without the optional
  # Postgres dependencies, mirroring a host application that uses only the
  # dependency-free core. See CONTRIBUTING.md.
  @core_only? System.get_env("DOCKET_CORE_ONLY") in ["1", "true"]

  def project do
    [
      app: :docket,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Durable, graph-based workflow execution for long-running, " <>
          "interruptible work like agentic LLM sessions.",
      docs: docs(),
      package: package(),
      source_url: @source_url
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
    [
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false, warn_if_outdated: true}
    ] ++ postgres_deps()
  end

  # The Postgres backend (`Docket.Postgres.*`) compiles only when the host
  # application already depends on ecto_sql and postgrex; core-only hosts
  # pull in nothing beyond telemetry.
  defp postgres_deps do
    if @core_only? do
      []
    else
      [
        {:ecto_sql, "~> 3.10", optional: true},
        {:postgrex, "~> 0.17", optional: true}
      ]
    end
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md CHANGELOG.md LICENSE docs examples)
    ]
  end

  defp docs do
    extras =
      [{"README.md", filename: "readme"}, {"CHANGELOG.md", []}] ++
        extra_pages("docs/*.md", "") ++
        extra_pages("docs/architecture/*.md", "architecture-") ++
        extra_pages("examples/*.md", "example-")

    [
      main: "readme",
      extras: extras,
      skip_undefined_reference_warnings_on: [
        "README.md",
        "CHANGELOG.md",
        "docs/architecture/README.md",
        "docs/architecture/docket-compiler-design.md",
        "docs/architecture/docket-graph-execution-contract-design.md",
        "docs/architecture/docket-runtime-design.md",
        "docs/architecture/docket-v0.1.0-spec-lock-audit.md"
      ]
    ]
  end

  defp extra_pages(pattern, prefix) do
    Enum.map(Path.wildcard(pattern), fn path ->
      {path, filename: prefix <> Path.basename(path, ".md")}
    end)
  end
end
