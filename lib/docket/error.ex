defmodule Docket.Error do
  @moduledoc """
  Public typed error returned by runtime and execution APIs.

  `type` identifies the error family; `phase` narrows where it occurred when
  useful (for example `:run_initialized` for a failed initialization
  checkpoint). `node_id` is the public graph node ID when the error is
  node-scoped.
  """

  defexception [:type, :phase, :node_id, :reason, :message, details: %{}]

  @type t :: %__MODULE__{
          type: atom(),
          phase: atom() | nil,
          node_id: String.t() | nil,
          reason: term(),
          message: String.t(),
          details: map()
        }

  @impl true
  def exception(opts) when is_list(opts) do
    type = Keyword.fetch!(opts, :type)
    message = Keyword.fetch!(opts, :message)

    %__MODULE__{
      type: type,
      phase: Keyword.get(opts, :phase),
      node_id: Keyword.get(opts, :node_id),
      reason: Keyword.get(opts, :reason),
      message: message,
      details: Keyword.get(opts, :details, %{})
    }
  end

  @doc false
  @spec new(atom(), String.t(), keyword()) :: t()
  def new(type, message, opts \\ []) do
    exception([type: type, message: message] ++ opts)
  end
end
