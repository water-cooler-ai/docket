defmodule Docket.Graph.Compiler.Diagnostics do
  @moduledoc false

  # Diagnostic builders shared by compiler passes. Representable graph
  # invalidity surfaces through these values, never through exceptions.

  alias Docket.Graph.Diagnostic

  @spec error(atom(), String.t(), keyword()) :: Diagnostic.t()
  def error(code, message, opts \\ []), do: build(:error, code, message, opts)

  @spec warning(atom(), String.t(), keyword()) :: Diagnostic.t()
  def warning(code, message, opts \\ []), do: build(:warning, code, message, opts)

  @spec blocking?([Diagnostic.t()]) :: boolean()
  def blocking?(diagnostics), do: Enum.any?(diagnostics, &(&1.severity == :error))

  defp build(severity, code, message, opts) do
    %Diagnostic{
      severity: severity,
      code: code,
      message: message,
      path: Keyword.get(opts, :path, []),
      public_id: Keyword.get(opts, :public_id),
      runtime_id: Keyword.get(opts, :runtime_id),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end
end
