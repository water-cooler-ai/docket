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

  `transition_id` is stable for one logical transition. An implementation must
  accept an exact replay as success and reject reuse of the ID with different
  canonical proposal/event content as `:conflict`. Canonical event replay
  compares the complete event value, not only `{run_id, seq}`.

  Implementations validate the complete proposal and portable size limits
  before writing. Wrong-tenant and unknown resources both return `:not_found`.
  Validation precedes lookup, immutable identity precedes fences, and event
  validation precedes publication. A failed operation publishes no run,
  schedule, support, event, or receipt changes.

  The portable error algebra is:

  * `:not_found` — the scoped graph/run is absent (including tenant concealment);
  * `:invalid_transition` — malformed proposal, immutable mismatch, or invalid
    schedule/event identity;
  * `:conflict` — a checkpoint fence lost or a transition ID was reused with
    different content;
  * `:event_conflict` — an existing event sequence has different canonical
    content;
  * `:too_large` — a portable/backend limit was exceeded before any write;
  * `{:retryable, reason}` — an infrastructure failure that is safe to retry
    with the same transition ID and content.

  Backends may document stricter limits. Every declared v1 implementation must
  satisfy at least `portable_limits/0`.
  """

  @version 1

  @portable_limits %{
    max_run_bytes: 350_000,
    max_events: 100,
    max_event_bytes: 64_000,
    max_transition_bytes: 3_500_000
  }

  @type ctx :: Docket.Backend.ctx()
  @type scope :: Docket.Backend.scope()
  @type owner_scope :: Docket.Backend.owner_scope()
  @type transition_id :: nonempty_binary()
  @type schedule :: Docket.Backend.RunStore.schedule()

  @typedoc "Portable limits every version-1 transition backend must meet."
  @type limits :: %{
          required(:max_run_bytes) => pos_integer(),
          required(:max_events) => pos_integer(),
          required(:max_event_bytes) => pos_integer(),
          required(:max_transition_bytes) => pos_integer()
        }

  @typedoc "Data-only initialization proposal."
  @type init_proposal :: %{
          required(:transition_id) => transition_id(),
          required(:run) => Docket.Run.t(),
          required(:checkpoint_type) => Docket.Checkpoint.type(),
          required(:wake_at) => DateTime.t()
        }

  @typedoc "Data-only claim-fenced transition proposal."
  @type claimed_proposal :: %{
          required(:transition_id) => transition_id(),
          required(:run) => Docket.Run.t(),
          required(:expected_checkpoint_seq) => non_neg_integer(),
          required(:claim_token) => Docket.Backend.RunStore.claim_token(),
          required(:checkpoint_type) => Docket.Checkpoint.type(),
          required(:schedule) => schedule()
        }

  @typedoc "Data-only optimistic transition proposal."
  @type unclaimed_proposal :: %{
          required(:transition_id) => transition_id(),
          required(:run) => Docket.Run.t(),
          required(:checkpoint_type) => Docket.Checkpoint.type(),
          required(:schedule) => schedule()
        }

  @type error_reason ::
          :not_found
          | :invalid_transition
          | :conflict
          | :event_conflict
          | :too_large
          | {:retryable, term()}

  @type result :: {:ok, Docket.Run.t()} | {:error, error_reason()}

  @doc "Atomically creates an initialized run and its assigned events."
  @callback initialize(ctx(), owner_scope(), init_proposal(), [Docket.Event.t()]) :: result()

  @doc "Atomically publishes one claim-fenced transition and its assigned events."
  @callback commit_claimed(ctx(), scope(), claimed_proposal(), [Docket.Event.t()]) :: result()

  @doc """
  Atomically publishes one unclaimed transition under a checkpoint CAS fence.

  Core may refetch and re-evaluate the pure mutation when this returns
  `{:error, :conflict}`. Mutation evaluation must therefore be deterministic,
  bounded, and free of external side effects. No-change and mutation errors do
  not invoke this callback and publish nothing.
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

  @spec portable_limits() :: limits()
  def portable_limits, do: @portable_limits

  @doc false
  @spec validate_limits(map(), [Docket.Event.t()], limits()) ::
          :ok | {:error, :too_large | :invalid_transition}
  def validate_limits(
        %{transition_id: transition_id, run: %Docket.Run{} = run} = proposal,
        events,
        limits
      )
      when is_binary(transition_id) and byte_size(transition_id) > 0 and is_list(events) and
             is_map(limits) do
    run_bytes = encoded_size(run)
    event_sizes = Enum.map(events, &encoded_size/1)
    transition_bytes = encoded_size(proposal) + Enum.sum(event_sizes)

    if run_bytes <= limits.max_run_bytes and
         length(events) <= limits.max_events and
         Enum.all?(event_sizes, &(&1 <= limits.max_event_bytes)) and
         transition_bytes <= limits.max_transition_bytes do
      :ok
    else
      {:error, :too_large}
    end
  end

  def validate_limits(_proposal, _events, _limits), do: {:error, :invalid_transition}

  @doc false
  @spec transition_id(:initialize | :claimed | :unclaimed, Docket.Run.t()) :: transition_id()
  def transition_id(kind, %Docket.Run{id: run_id, checkpoint_seq: checkpoint_seq})
      when kind in [:initialize, :claimed, :unclaimed] do
    "docket:v1:#{kind}:#{run_id}:#{checkpoint_seq}"
  end

  @doc false
  @spec digest(map(), [Docket.Event.t()]) :: binary()
  def digest(proposal, events) do
    :crypto.hash(
      :sha256,
      :erlang.term_to_binary({proposal, events}, [:deterministic, minor_version: 2])
    )
  end

  defp encoded_size(term) do
    term
    |> :erlang.term_to_binary([:deterministic, minor_version: 2])
    |> byte_size()
  end
end
