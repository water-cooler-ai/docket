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

  defmodule AtomKeyedConfigSchema do
    @moduledoc false
    # Schema.object accepts atom field keys; the compiler must canonicalize
    # them to strings so they line up with canonicalized node config.
    @behaviour Docket.Node

    @impl true
    def config_schema do
      Docket.Schema.object(%{
        tone: Docket.Schema.string(required: true),
        temperature: Docket.Schema.float(default: 0.5)
      })
    end

    @impl true
    def call(_state, _config, _context), do: {:ok, %{}}
  end

  defmodule StatefulConfigSchema do
    @moduledoc false
    # Returns a valid schema on the first call in a process and raises on
    # every later call. Proves the compiler fetches config schemas exactly
    # once per compile and surfaces later failures as diagnostics.
    @behaviour Docket.Node

    @impl true
    def config_schema do
      calls = Process.get({__MODULE__, :calls}, 0)
      Process.put({__MODULE__, :calls}, calls + 1)

      if calls == 0 do
        Docket.Schema.object(%{})
      else
        raise "config_schema/0 was invoked more than once"
      end
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

  defmodule WriteValue do
    @moduledoc false
    # WriteStatic for non-string values: the open config schema accepts any
    # durable "value".
    @behaviour Docket.Node

    @impl true
    def config_schema do
      Docket.Schema.object(%{"field" => Docket.Schema.string(required: true)}, open: true)
    end

    @impl true
    def call(_state, config, _context) do
      {:ok, %{config["field"] => config["value"]}}
    end
  end

  defmodule InterruptWhileEmpty do
    @moduledoc false
    # InterruptOnce for accumulating resume fields, whose effective default
    # makes the field always present in snapshots: interrupts while the
    # resume field is empty, then copies it into the write field.
    @behaviour Docket.Node

    @impl true
    def config_schema do
      Docket.Schema.object(%{
        "resume_field" => Docket.Schema.string(required: true),
        "write_field" => Docket.Schema.string(required: true)
      })
    end

    @impl true
    def call(state, config, _context) do
      case Map.get(state, config["resume_field"]) do
        empty when empty in [nil, [], %{}] ->
          {:interrupt,
           %Docket.Interrupt{
             prompt: "value for #{config["resume_field"]}?",
             resume_channel: config["resume_field"]
           }}

        value ->
          {:ok, %{config["write_field"] => value}}
      end
    end
  end

  defmodule Increment do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema do
      Docket.Schema.object(%{"field" => Docket.Schema.string(required: true)})
    end

    @impl true
    def call(state, config, _context) do
      {:ok, %{config["field"] => (Map.get(state, config["field"]) || 0) + 1}}
    end
  end

  defmodule InterruptOnce do
    @moduledoc false
    # Interrupts while the resume field is unwritten; on re-execution after
    # resolution it copies the resolved value into the write field.
    @behaviour Docket.Node

    @impl true
    def config_schema do
      Docket.Schema.object(%{
        "resume_field" => Docket.Schema.string(required: true),
        "write_field" => Docket.Schema.string(required: true)
      })
    end

    @impl true
    def call(state, config, _context) do
      case Map.fetch(state, config["resume_field"]) do
        :error ->
          {:interrupt,
           %Docket.Interrupt{
             prompt: "value for #{config["resume_field"]}?",
             schema: Docket.Schema.string(),
             resume_channel: config["resume_field"]
           }}

        {:ok, value} ->
          {:ok, %{config["write_field"] => value}}
      end
    end
  end

  defmodule FlakyThenSucceeds do
    @moduledoc false
    # Fails deterministically while context.attempt <= config failures, then
    # writes the configured value. Schema has no integer type; "failures" is
    # a float compared against the integer attempt counter.
    @behaviour Docket.Node

    @impl true
    def config_schema do
      Docket.Schema.object(%{
        "failures" => Docket.Schema.float(required: true),
        "field" => Docket.Schema.string(required: true),
        "value" => Docket.Schema.string(required: true)
      })
    end

    @impl true
    def call(_state, config, context) do
      if context.attempt <= trunc(config["failures"]) do
        {:error, {:flaky, context.attempt}}
      else
        {:ok, %{config["field"] => config["value"]}}
      end
    end
  end

  defmodule AlwaysFails do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema, do: Docket.Schema.object(%{})

    @impl true
    def call(_state, _config, _context), do: {:error, :always_fails}
  end

  defmodule Raises do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema, do: Docket.Schema.object(%{})

    @impl true
    def call(_state, _config, _context), do: raise("node exploded")
  end

  defmodule Throws do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema, do: Docket.Schema.object(%{})

    @impl true
    def call(_state, _config, _context), do: throw(:ball)
  end

  defmodule Awaits do
    @moduledoc false
    # {:await, _} is a reserved post-v1 return shape.
    @behaviour Docket.Node

    @impl true
    def config_schema, do: Docket.Schema.object(%{})

    @impl true
    def call(_state, _config, _context), do: {:await, :external}
  end

  defmodule SleepsUntilReleased do
    @moduledoc false
    # Announces itself to the test coordinator and blocks until released - no
    # wall-clock sleep. Tests release it deterministically with
    # `send(pid, :release)` or let a "timeout_ms" policy kill the attempt.
    @behaviour Docket.Node

    @impl true
    def config_schema do
      Docket.Schema.object(%{
        "field" => Docket.Schema.string(required: true),
        "value" => Docket.Schema.string(required: true)
      })
    end

    @impl true
    def call(_state, config, context) do
      coordinator = Map.fetch!(context.application, :coordinator)
      send(coordinator, {:blocked, self(), context.node_id, context.attempt})

      receive do
        :release -> {:ok, %{config["field"] => config["value"]}}
      end
    end
  end

  defmodule AtomWriter do
    @moduledoc false
    # Returns atom-keyed/atom-valued content; the update barrier coerces it
    # to durable string form exactly as a checkpointed run would persist it.
    @behaviour Docket.Node

    @impl true
    def config_schema, do: Docket.Schema.object(%{})

    @impl true
    def call(_state, _config, _context), do: {:ok, %{out: %{status: :ok}}}
  end

  defmodule BadReturn do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema, do: Docket.Schema.object(%{})

    @impl true
    def call(_state, _config, _context), do: :oops
  end
end
