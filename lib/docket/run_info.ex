defmodule Docket.RunInfo do
  @moduledoc """
  Substrate-neutral operational projection of one durable run.

  Operational health lives outside `Docket.Run`: the run document describes
  graph execution, while this projection adds what the backend knows about
  scheduling and delivery health. Backends return it from `inspect_run`; it
  never exposes the claim token.

  Fields:

  - `run` - the last committed `Docket.Run`.
  - `wake_at` - when the run next advances autonomously: a past-or-present
    instant means runnable, a future instant means a timer or retry backoff,
    and `nil` means claimed, externally parked, poisoned, or terminal.
  - `claimed_at` - when the current execution claim was acquired or last
    refreshed, or `nil` when unclaimed.
  - `claim_attempts` - consecutive claims launched without committed
    progress; resets to zero on any committed run mutation.
  - `poisoned_at` / `poison_reason` - paired poison facts, both `nil` for a
    healthy run. A poisoned run is excluded from dispatch until an operator
    (or `retry_poisoned_run`) recovers it.

  ## `inspect_run`

  `inspect_run(run_id, opts)` is the operational read: it returns
  `{:ok, %Docket.RunInfo{}}` for the committed run plus the fields above, or
  `{:error, :not_found}` for an unknown or out-of-scope run. `fetch_run`
  remains the committed run-document read and returns only the `Docket.Run`.

  ## `await_run`

  `await_run(run_id, opts)` blocks until the run reaches a terminal status,
  parks waiting on input, becomes poisoned, or the required `:timeout`
  elapses - whichever comes first:

  - terminal or waiting: `{:ok, %Docket.Run{}}` as of that boundary
  - poisoned: `{:error, {:poisoned, %Docket.RunInfo{}}}` - the typed
    operational halt. Awaiting stops as soon as poison facts are present
    rather than polling until timeout, because a poisoned run makes no
    autonomous progress until it is recovered.
  - timeout: `{:error, :timeout}`
  """

  @enforce_keys [:run]
  defstruct [:run, :wake_at, :claimed_at, :poisoned_at, :poison_reason, claim_attempts: 0]

  @type t :: %__MODULE__{
          run: Docket.Run.t(),
          wake_at: DateTime.t() | nil,
          claimed_at: DateTime.t() | nil,
          claim_attempts: non_neg_integer(),
          poisoned_at: DateTime.t() | nil,
          poison_reason: String.t() | nil
        }

  @typedoc "Typed operational halt returned by a poisoned `await_run`."
  @type await_halt :: {:poisoned, t()}

  @doc """
  Builds a projection from a map or keyword list, validating field shapes.

  Requires `:run`; `poisoned_at` and `poison_reason` must be present
  together. Raises `ArgumentError` on malformed input.
  """
  @spec new!(map() | keyword()) :: t()
  def new!(fields) when is_list(fields), do: fields |> Map.new() |> new!()

  def new!(fields) when is_map(fields) and not is_struct(fields) do
    info = struct!(__MODULE__, fields)

    unless is_struct(info.run, Docket.Run) do
      raise ArgumentError, "run info run must be a Docket.Run, got: #{inspect(info.run)}"
    end

    unless is_integer(info.claim_attempts) and info.claim_attempts >= 0 do
      raise ArgumentError,
            "run info claim_attempts must be a non-negative integer, got: #{inspect(info.claim_attempts)}"
    end

    validate_optional_timestamp!(info.wake_at, :wake_at)
    validate_optional_timestamp!(info.claimed_at, :claimed_at)
    validate_poison_facts!(info.poisoned_at, info.poison_reason)

    info
  end

  @doc """
  Returns true when the projection carries current poison facts.
  """
  @spec poisoned?(t()) :: boolean()
  def poisoned?(%__MODULE__{poisoned_at: %DateTime{}}), do: true
  def poisoned?(%__MODULE__{}), do: false

  defp validate_optional_timestamp!(nil, _field), do: :ok
  defp validate_optional_timestamp!(%DateTime{}, _field), do: :ok

  defp validate_optional_timestamp!(other, field) do
    raise ArgumentError, "run info #{field} must be a DateTime or nil, got: #{inspect(other)}"
  end

  defp validate_poison_facts!(nil, nil), do: :ok

  defp validate_poison_facts!(%DateTime{}, reason) when is_binary(reason) and reason != "",
    do: :ok

  defp validate_poison_facts!(poisoned_at, poison_reason) do
    raise ArgumentError,
          "run info poison facts must be paired: a poisoned run has both poisoned_at and " <>
            "poison_reason, a healthy run has neither, got: " <>
            "#{inspect(poisoned_at: poisoned_at, poison_reason: poison_reason)}"
  end
end
