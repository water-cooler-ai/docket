if Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Benchmark.Postgres.Database do
    @moduledoc false

    @doc "Builds a unique isolated database name and URL for one benchmark point."
    def isolated(database_url) do
      uri = URI.parse(database_url)

      name =
        "docket_bench_#{System.system_time(:millisecond)}_#{System.unique_integer([:positive])}"

      %{name: name, url: %{uri | path: "/" <> name} |> URI.to_string()}
    end

    @doc "Returns Postgrex connection options for the administrative database."
    def primary_config(url) do
      uri = URI.parse(url)

      [username, password] =
        case String.split(uri.userinfo || System.get_env("USER") || "postgres", ":", parts: 2) do
          [username, password] -> [URI.decode(username), URI.decode(password)]
          [username] -> [URI.decode(username), nil]
        end

      [
        hostname: uri.host || "localhost",
        port: uri.port || 5432,
        username: username,
        password: password,
        database: "postgres",
        pool_size: 1
      ]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    end

    @doc "Creates an isolated benchmark database."
    def create!(config, name) do
      {:ok, pid} = Postgrex.start_link(config)

      try do
        Postgrex.query!(pid, "CREATE DATABASE \"#{name}\"", [])
      after
        GenServer.stop(pid)
      end
    end

    @doc "Drops an isolated benchmark database and reports cleanup failures."
    def drop(config, name) do
      try do
        do_drop(config, name)
      rescue
        error -> {:error, "failed to remove isolated benchmark database: #{error_message(error)}"}
      catch
        kind, reason ->
          {:error,
           "failed to remove isolated benchmark database: #{Exception.format_banner(kind, reason)}"}
      end
    end

    defp do_drop(config, name) do
      case Postgrex.start_link(config) do
        {:ok, pid} ->
          try do
            case Postgrex.query(pid, "DROP DATABASE IF EXISTS \"#{name}\" WITH (FORCE)", []) do
              {:ok, _result} ->
                :ok

              {:error, reason} ->
                {:error, "failed to drop isolated benchmark database: #{error_message(reason)}"}
            end
          after
            GenServer.stop(pid)
          end

        {:error, reason} ->
          {:error,
           "failed to connect for isolated benchmark database cleanup: #{error_message(reason)}"}
      end
    end

    defp error_message(%{__struct__: module} = error) do
      if function_exported?(module, :message, 1),
        do: Exception.message(error),
        else: inspect(error)
    end

    defp error_message(reason), do: inspect(reason)
  end
end
