defmodule Docket.Graph.Error do
  @moduledoc """
  Public graph construction and editing error.
  """

  defexception [:code, :message, details: %{}]

  @type t :: %__MODULE__{
          code: atom(),
          message: String.t(),
          details: map()
        }

  @impl true
  def exception(message) when is_binary(message) do
    %__MODULE__{code: :graph_error, message: message, details: %{}}
  end

  def exception(opts) when is_list(opts) do
    code = Keyword.fetch!(opts, :code)
    message = Keyword.fetch!(opts, :message)
    details = Keyword.get(opts, :details, %{})

    %__MODULE__{code: code, message: message, details: details}
  end
end
