defmodule Docket.Backend.TransitionStore do
  @moduledoc """
  Versioned semantic write contract for durable lifecycle transitions.

  A transition store receives complete, substrate-neutral proposals. It owns
  the native atomic primitive used to validate and publish each proposal; core
  never passes an Elixir callback, transaction handle, or
  `Docket.Runtime.Moment` through this contract.

  Version 1 defines three operations:

  * `initialize/4` creates an initial run, its schedule/supporting state, and
    assigned events after validating the owner-scoped graph precondition.
  * `commit_claimed/4` advances a run under both the checkpoint-sequence and
    claim-token fences.
  * `commit_unclaimed/5` applies a signal/admin proposal under an optimistic
    checkpoint-sequence fence and never requires a claim.

  Implementations validate the complete proposal before writing. Wrong-tenant
  and unknown resources both return `:not_found`. Validation precedes lookup,
  immutable identity precedes fences, and event validation precedes
  publication. A failed operation publishes no run, schedule, support, or
  event changes. Events are idempotent by canonical content at
  `{run_id, seq}`: a pre-existing identical event is accepted, and different
  content at a stored sequence returns `:event_conflict`.

  The portable error algebra is:

  * `:not_found` — the scoped graph/run is absent (including tenant
    concealment);
  * `:invalid_transition` — malformed proposal, immutable mismatch, or invalid
    schedule/event identity;
  * `:stale_checkpoint` — a claim-token or checkpoint fence lost; core may
    refetch and re-evaluate a pure unclaimed mutation;
  * `:conflict` — the proposed run already exists in the owner scope;
  * `:event_conflict` — an existing event sequence has different canonical
    content;
  * `{:retryable, reason}` — an infrastructure failure that is safe to retry;
  * `{:permanent, reason}` — a non-retryable infrastructure failure.
  """

  @version 1

  @type ctx :: Docket.Backend.ctx()
  @type scope :: Docket.Backend.scope()
  @type owner_scope :: Docket.Backend.owner_scope()

  @typedoc """
  Storage effect applied with a committed run transition.

  `:retain_claim` keeps the current token, refreshes its claimed time, and
  leaves the run without a wake. A release clears the token and claimed time.
  `:immediate` records a wake at the backend's current time, `{:at, time}`
  records a future or current wake, and `:external` or `:terminal` records no
  wake. The two nil-wake reasons remain distinct here so implementations can
  validate the proposed run status.
  """
  @type schedule ::
          :retain_claim
          | {:release_claim, :immediate | :external | :terminal | {:at, DateTime.t()}}

  @typedoc """
  Data-only initialization proposal.

  * `:run` — the complete initial run value; its `checkpoint_seq` is at
    least 1.
  * `:checkpoint_type` — must be `:run_initialized`.
  * `:wake_at` — the run's first explicit schedule.
  """
  @type init_proposal :: %{
          required(:run) => Docket.Run.t(),
          required(:checkpoint_type) => Docket.Checkpoint.type(),
          required(:wake_at) => DateTime.t()
        }

  @typedoc """
  Data-only claim-fenced transition proposal.

  * `:run` — the complete next run value; its `checkpoint_seq` must equal
    `expected_checkpoint_seq + 1`.
  * `:expected_checkpoint_seq` — the committed checkpoint sequence this
    transition fences against.
  * `:claim_token` — the non-empty claim token that must still hold the run.
  * `:checkpoint_type` — the checkpoint type recorded for this transition.
  * `:schedule` — the claim disposition. `:retain_claim` requires a `:running`
    run; `{:release_claim, :immediate}` and `{:release_claim, {:at, at}}`
    require `:running`; `{:release_claim, :external}` requires `:waiting`;
    `{:release_claim, :terminal}` requires `:done`, `:failed`, or
    `:cancelled`.
  """
  @type claimed_proposal :: %{
          required(:run) => Docket.Run.t(),
          required(:expected_checkpoint_seq) => non_neg_integer(),
          required(:claim_token) => Docket.Backend.RunStore.claim_token(),
          required(:checkpoint_type) => Docket.Checkpoint.type(),
          required(:schedule) => schedule()
        }

  @typedoc """
  Data-only optimistic transition proposal.

  Carries the same fields as `t:claimed_proposal/0` except the claim token,
  which unclaimed transitions never require, and the expected checkpoint
  sequence, which `c:commit_unclaimed/5` receives as its own argument because
  it is also the compare-and-swap input core re-reads on `:stale_checkpoint`.
  `:retain_claim` is not a valid unclaimed schedule.
  """
  @type unclaimed_proposal :: %{
          required(:run) => Docket.Run.t(),
          required(:checkpoint_type) => Docket.Checkpoint.type(),
          required(:schedule) => schedule()
        }

  @type error_reason ::
          :not_found
          | :invalid_transition
          | :stale_checkpoint
          | :conflict
          | :event_conflict
          | {:retryable, term()}
          | {:permanent, term()}

  @type result :: {:ok, Docket.Run.t()} | {:error, error_reason()}

  @doc "Atomically creates an initialized run and its assigned events."
  @callback initialize(ctx(), owner_scope(), init_proposal(), [Docket.Event.t()]) :: result()

  @doc "Atomically publishes one claim-fenced transition and its assigned events."
  @callback commit_claimed(ctx(), scope(), claimed_proposal(), [Docket.Event.t()]) :: result()

  @doc """
  Atomically publishes one unclaimed transition under a checkpoint CAS fence.

  Core may refetch and re-evaluate the pure mutation when this returns
  `{:error, :stale_checkpoint}`. Mutation evaluation must therefore be
  deterministic, bounded, and free of external side effects. No-change and
  mutation errors do not invoke this callback and publish nothing.
  """
  @callback commit_unclaimed(
              ctx(),
              scope(),
              expected_checkpoint_seq :: non_neg_integer(),
              unclaimed_proposal(),
              [Docket.Event.t()]
            ) :: result()

  @spec version() :: pos_integer()
  def version, do: @version

  @doc false
  @spec validate(
          :initialize | :claimed | :unclaimed,
          non_neg_integer(),
          map(),
          [Docket.Event.t()]
        ) :: :ok | {:error, :invalid_transition | :event_conflict}
  def validate(kind, expected_checkpoint_seq, proposal, events)
      when kind in [:initialize, :claimed, :unclaimed] and
             is_integer(expected_checkpoint_seq) and expected_checkpoint_seq >= 0 do
    with :ok <- validate_proposal(kind, expected_checkpoint_seq, proposal),
         :ok <- validate_events(proposal, events) do
      :ok
    end
  end

  def validate(_kind, _expected_checkpoint_seq, _proposal, _events),
    do: {:error, :invalid_transition}

  defp validate_proposal(
         :initialize,
         0,
         %{
           run: %Docket.Run{} = run,
           checkpoint_type: :run_initialized,
           wake_at: %DateTime{}
         }
       ) do
    if run.status == :running and run.checkpoint_seq >= 1 and
         Docket.Run.validate_durable(run) == :ok,
       do: :ok,
       else: {:error, :invalid_transition}
  end

  defp validate_proposal(
         :claimed,
         expected_checkpoint_seq,
         %{
           run: %Docket.Run{} = run,
           expected_checkpoint_seq: expected_checkpoint_seq,
           claim_token: claim_token,
           checkpoint_type: checkpoint_type,
           schedule: schedule
         }
       )
       when is_binary(claim_token) and byte_size(claim_token) > 0 do
    if run.checkpoint_seq == expected_checkpoint_seq + 1 and
         checkpoint_type in Docket.Checkpoint.types() and
         valid_schedule?(schedule, run.status, true) and
         Docket.Run.validate_durable(run) == :ok,
       do: :ok,
       else: {:error, :invalid_transition}
  end

  defp validate_proposal(
         :unclaimed,
         expected_checkpoint_seq,
         %{
           run: %Docket.Run{} = run,
           checkpoint_type: checkpoint_type,
           schedule: schedule
         }
       ) do
    if run.checkpoint_seq == expected_checkpoint_seq + 1 and
         checkpoint_type in Docket.Checkpoint.types() and
         valid_schedule?(schedule, run.status, false) and
         Docket.Run.validate_durable(run) == :ok,
       do: :ok,
       else: {:error, :invalid_transition}
  end

  defp validate_proposal(_kind, _expected_checkpoint_seq, _proposal),
    do: {:error, :invalid_transition}

  defp validate_events(%{run: %Docket.Run{} = run}, events) do
    case canonical_events(events, run.id) do
      {:ok, _by_sequence} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_events(_proposal, _events), do: {:error, :invalid_transition}

  defp canonical_events(events, run_id) when is_list(events) do
    Enum.reduce_while(events, {:ok, %{}}, fn
      %Docket.Event{
        run_id: ^run_id,
        seq: seq,
        timestamp: %DateTime{}
      } = event,
      {:ok, accumulated}
      when is_integer(seq) and seq > 0 ->
        case Map.fetch(accumulated, seq) do
          :error ->
            {:cont, {:ok, Map.put(accumulated, seq, event)}}

          {:ok, ^event} ->
            {:cont, {:ok, accumulated}}

          {:ok, _different} ->
            {:halt, {:error, :event_conflict}}
        end

      _event, _accumulated ->
        {:halt, {:error, :invalid_transition}}
    end)
  end

  defp canonical_events(_events, _run_id), do: {:error, :invalid_transition}

  defp valid_schedule?(:retain_claim, :running, true), do: true
  defp valid_schedule?({:release_claim, :immediate}, :running, _claimed), do: true
  defp valid_schedule?({:release_claim, {:at, %DateTime{}}}, :running, _claimed), do: true
  defp valid_schedule?({:release_claim, :external}, :waiting, _claimed), do: true

  defp valid_schedule?({:release_claim, :terminal}, status, _claimed),
    do: status in [:done, :failed, :cancelled]

  defp valid_schedule?(_schedule, _status, _claimed), do: false
end
