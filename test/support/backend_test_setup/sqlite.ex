if Code.ensure_loaded?(Exqlite.Sqlite3) do
  defmodule Docket.Test.BackendTestSetup.SQLite do
    @moduledoc false

    @now ~U[2026-07-15 12:00:00.000000Z]

    def setup_suite, do: {:ok, []}

    def setup(_context) do
      name = Module.concat(__MODULE__, "Conn#{System.unique_integer([:positive])}")

      ExUnit.Callbacks.start_supervised!(
        {Docket.SQLite.Connection, name: name, database: ":memory:"}
      )

      subject = %{
        backend: Docket.SQLite,
        context: %{
          conn: name,
          runner: nil,
          clock: fn -> @now end,
          token_generator: fn ->
            18 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
          end
        },
        namespace: "sqlite-#{System.unique_integer([:positive, :monotonic])}",
        now: @now
      }

      {:ok, backend_test: subject}
    end
  end
end
