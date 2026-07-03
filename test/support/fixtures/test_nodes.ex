defmodule Docket.Test.Fixtures.Nodes do
  @moduledoc """
  Executable `Docket.Node` fixtures used by compiler and runtime tests.

  These modules perform no side effects. Compiler tests only introspect their
  contracts; inline runtime tests will execute their `call/3` callbacks later.
  """

  defmodule CopyInput do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema do
      Docket.Schema.object(%{
        "from" => Docket.Schema.string(required: true),
        "to" => Docket.Schema.string(required: true)
      })
    end

    @impl true
    def call(state, config, _context) do
      {:ok, %{config["to"] => Map.get(state, config["from"])}}
    end
  end

  defmodule Echo do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema, do: Docket.Schema.object(%{})

    @impl true
    def call(_state, _config, _context), do: {:ok, %{}}
  end

  defmodule WriteStatic do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema do
      Docket.Schema.object(%{
        "field" => Docket.Schema.string(required: true),
        "value" => Docket.Schema.string(required: true)
      })
    end

    @impl true
    def call(_state, config, _context) do
      {:ok, %{config["field"] => config["value"]}}
    end
  end

  defmodule WithDefaults do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema do
      Docket.Schema.object(%{
        "tone" => Docket.Schema.string(required: true),
        "temperature" => Docket.Schema.float(default: 0.5)
      })
    end

    @impl true
    def call(_state, _config, _context), do: {:ok, %{}}
  end

  defmodule RaisingConfigSchema do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema, do: raise("config_schema exploded")

    @impl true
    def call(_state, _config, _context), do: {:ok, %{}}
  end

  defmodule MalformedConfigSchema do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema, do: :not_a_schema

    @impl true
    def call(_state, _config, _context), do: {:ok, %{}}
  end

  defmodule NotANode do
    @moduledoc false
    # Loads fine but exports neither config_schema/0 nor call/3.
    def unrelated, do: :ok
  end
end
