defmodule Docket.Run.Failure do
  @moduledoc """
  Durable, JSON-safe description of a terminal graph failure.

  A failure is present on a `Docket.Run` exactly when the run's status is
  `:failed`, and it survives storage round trips even when event persistence
  is disabled. It describes only the terminal graph outcome: retryable
  node-attempt failures, operational poison facts, API validation errors,
  fence loss, and observer failures are separate concerns and never appear
  here.

  Fields:

  - `code` - stable, machine-matchable identifier for the failure class.
    Codes produced by Docket are stable across releases.
  - `message` - human-readable description.
  - `node_id` - the failing node, when the failure is attributable to
    exactly one node.
  - `details` - open JSON-safe map with failure-class-specific facts.
  """

  @enforce_keys [:code, :message]
  defstruct [:code, :message, :node_id, details: %{}]

  @type t :: %__MODULE__{
          code: String.t(),
          message: String.t(),
          node_id: String.t() | nil,
          details: map()
        }

  @doc """
  Builds a failure, validating field shapes.

  `code` and `message` must be non-empty strings. Options: `:node_id`
  (string) and `:details` (string-keyed map). Raises `ArgumentError` on
  malformed input.
  """
  @spec new(String.t(), String.t(), keyword()) :: t()
  def new(code, message, opts \\ []) do
    unless nonempty_string?(code) do
      raise ArgumentError, "failure code must be a non-empty string, got: #{inspect(code)}"
    end

    unless nonempty_string?(message) do
      raise ArgumentError, "failure message must be a non-empty string, got: #{inspect(message)}"
    end

    node_id = Keyword.get(opts, :node_id)

    unless is_nil(node_id) or nonempty_string?(node_id) do
      raise ArgumentError, "failure node_id must be a non-empty string, got: #{inspect(node_id)}"
    end

    details = Keyword.get(opts, :details, %{})

    unless is_map(details) and not is_struct(details) do
      raise ArgumentError, "failure details must be a plain map, got: #{inspect(details)}"
    end

    %__MODULE__{code: code, message: message, node_id: node_id, details: details}
  end

  defp nonempty_string?(value), do: is_binary(value) and byte_size(value) > 0
end
