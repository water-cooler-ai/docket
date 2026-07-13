defmodule Docket.RunSummary do
  @moduledoc """
  Lightweight projection of one durable run for collection reads.

  A summary contains the indexed, public run fields needed to identify and
  filter a run without loading or decoding its complete durable state. The
  tenant is included so trusted system-scoped callers can distinguish owners;
  tenant-scoped callers only ever receive their own rows.

  Backend-owned scheduling, claim, and poison details remain available through
  `Docket.RunInfo`. In particular, summaries never expose claim tokens.
  """

  @enforce_keys [
    :id,
    :tenant_id,
    :graph_id,
    :graph_hash,
    :status,
    :step,
    :checkpoint_seq,
    :started_at,
    :updated_at,
    :finished_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          id: String.t(),
          tenant_id: String.t() | nil,
          graph_id: String.t(),
          graph_hash: String.t(),
          status: Docket.Run.durable_status(),
          step: non_neg_integer(),
          checkpoint_seq: pos_integer(),
          started_at: DateTime.t(),
          updated_at: DateTime.t(),
          finished_at: DateTime.t() | nil
        }

  @doc """
  Builds a summary from a map or keyword list and validates its public shape.

  All fields must be present, including nullable `tenant_id` and
  `finished_at`. Raises `ArgumentError` for a missing or malformed field.
  """
  @spec new!(map() | keyword()) :: t()
  def new!(fields) when is_list(fields), do: fields |> Map.new() |> new!()

  def new!(fields) when is_map(fields) and not is_struct(fields) do
    summary = struct!(__MODULE__, fields)

    validate_nonempty_binary!(summary.id, :id)
    validate_optional_nonempty_binary!(summary.tenant_id, :tenant_id)
    validate_nonempty_binary!(summary.graph_id, :graph_id)
    validate_nonempty_binary!(summary.graph_hash, :graph_hash)

    unless Docket.Run.durable_status?(summary.status) do
      raise ArgumentError,
            "run summary status must be durable, got: #{inspect(summary.status)}"
    end

    validate_non_negative_integer!(summary.step, :step)
    validate_positive_integer!(summary.checkpoint_seq, :checkpoint_seq)
    validate_datetime!(summary.started_at, :started_at)
    validate_datetime!(summary.updated_at, :updated_at)
    validate_optional_datetime!(summary.finished_at, :finished_at)

    summary
  end

  def new!(fields) do
    raise ArgumentError,
          "run summary fields must be a map or keyword list, got: #{inspect(fields)}"
  end

  @doc "Returns the exact saved graph version referenced by the run."
  @spec graph_ref(t()) :: Docket.GraphRef.t()
  def graph_ref(%__MODULE__{graph_id: graph_id, graph_hash: graph_hash}) do
    %Docket.GraphRef{graph_id: graph_id, graph_hash: graph_hash}
  end

  defp validate_nonempty_binary!(value, _field) when is_binary(value) and byte_size(value) > 0,
    do: :ok

  defp validate_nonempty_binary!(value, field) do
    raise ArgumentError,
          "run summary #{field} must be a non-empty binary, got: #{inspect(value)}"
  end

  defp validate_optional_nonempty_binary!(nil, _field), do: :ok

  defp validate_optional_nonempty_binary!(value, field),
    do: validate_nonempty_binary!(value, field)

  defp validate_non_negative_integer!(value, _field) when is_integer(value) and value >= 0,
    do: :ok

  defp validate_non_negative_integer!(value, field) do
    raise ArgumentError,
          "run summary #{field} must be a non-negative integer, got: #{inspect(value)}"
  end

  defp validate_positive_integer!(value, _field) when is_integer(value) and value > 0, do: :ok

  defp validate_positive_integer!(value, field) do
    raise ArgumentError,
          "run summary #{field} must be a positive integer, got: #{inspect(value)}"
  end

  defp validate_datetime!(%DateTime{}, _field), do: :ok

  defp validate_datetime!(value, field) do
    raise ArgumentError, "run summary #{field} must be a DateTime, got: #{inspect(value)}"
  end

  defp validate_optional_datetime!(nil, _field), do: :ok
  defp validate_optional_datetime!(value, field), do: validate_datetime!(value, field)
end
