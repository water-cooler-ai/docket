defmodule Docket.Runtime.Graph.Lowering do
  @moduledoc """
  Required lowering metadata mapping runtime IDs back to public graph intent.

  Supports diagnostics, runtime debug views, live run overlays, event mapping,
  and test assertions. Branch groups do not lower to execution machinery in
  v1; they are preserved here for editors and overlays.
  """

  defstruct public_to_runtime: %{
              inputs: %{},
              fields: %{},
              nodes: %{},
              edges: %{},
              outputs: %{}
            },
            runtime_to_public: %{},
            generated: %{},
            branches: %{}

  @type public_kind :: :input | :field | :node | :edge | :output

  @type t :: %__MODULE__{
          public_to_runtime: %{
            inputs: %{optional(String.t()) => String.t()},
            fields: %{optional(String.t()) => String.t()},
            nodes: %{optional(String.t()) => String.t()},
            edges: %{optional(String.t()) => String.t()},
            outputs: %{optional(String.t()) => String.t()}
          },
          runtime_to_public: %{optional(String.t()) => {public_kind(), String.t()}},
          generated: %{optional(String.t()) => map()},
          branches: %{optional(String.t()) => %{optional(String.t()) => [String.t()]}}
        }
end
