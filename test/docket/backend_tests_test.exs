backends = [
  {Docket.Test.MemoryBackend, Docket.Test.BackendTestSetup.Memory, true, []}
]

backends =
  if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
    backends ++
      [{Docket.Postgres, Docket.Test.BackendTestSetup.Postgres, false, postgres: true}]
  else
    backends
  end

for {backend, setup_module, async?, tags} <- backends do
  defmodule Module.concat(backend, SharedBackendTest) do
    use ExUnit.Case, async: async?

    @backend_test_setup setup_module
    @moduletag tags

    setup_all do
      @backend_test_setup.setup_suite()
    end

    setup context do
      @backend_test_setup.setup(context)
    end

    use Docket.BackendTests
  end
end
