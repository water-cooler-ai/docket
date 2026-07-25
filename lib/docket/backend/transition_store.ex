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

  `transition_id` is stable for one logical transition. It is a non-empty
  UTF-8 binary of at most `max_transition_id_bytes/0` bytes so every portable
  substrate can index it. An implementation must accept a replay with
  identical canonical content as success and reject reuse of the ID with
  different canonical operation, fence, proposal, or event content as
  `:conflict`. Canonical comparison collapses duplicate identical events and
  is insensitive to event list order; it compares the complete event value,
  not only `{run_id, seq}`.

  Implementations validate the complete proposal and portable size limits
  before writing. Wrong-tenant and unknown resources both return `:not_found`.
  Validation precedes lookup, immutable identity precedes fences, and event
  validation precedes publication. A failed operation publishes no run,
  schedule, support, event, or receipt changes.

  The portable error algebra is:

  * `:not_found` — the scoped graph/run is absent (including tenant concealment);
  * `:invalid_transition` — malformed proposal, immutable mismatch, or invalid
    schedule/event identity;
  * `:stale_checkpoint` — an optimistic checkpoint or event-sequence fence
    lost; core may refetch and re-evaluate a pure unclaimed mutation;
  * `:conflict` — a transition ID was reused with different canonical content;
  * `:event_conflict` — an existing event sequence has different canonical
    content;
  * `:too_large` — a portable/backend limit was exceeded before any write;
  * `{:retryable, reason}` — an infrastructure failure that is safe to retry
    with the same transition ID and content;
  * `{:permanent, reason}` — a non-retryable infrastructure failure.

  Backends may document stricter limits. Every declared v1 implementation must
  satisfy at least `portable_limits/0`.
  """

  @version 1

  @max_transition_id_bytes 512

  @portable_limits %{
    max_run_bytes: 350_000,
    max_events: 100,
    max_event_bytes: 64_000,
    max_transition_bytes: 3_500_000
  }

  @type ctx :: Docket.Backend.ctx()
  @type scope :: Docket.Backend.scope()
  @type owner_scope :: Docket.Backend.owner_scope()

  @typedoc "Non-empty UTF-8 binary of at most `max_transition_id_bytes/0` bytes."
  @type transition_id :: nonempty_binary()
  @type schedule :: Docket.Backend.RunStore.schedule()

  @typedoc "Portable limits every version-1 transition backend must meet."
  @type limits :: %{
          required(:max_run_bytes) => pos_integer(),
          required(:max_events) => pos_integer(),
          required(:max_event_bytes) => pos_integer(),
          required(:max_transition_bytes) => pos_integer()
        }

  @typedoc """
  Data-only initialization proposal.

  * `:transition_id` — stable replay identity for this initialization.
  * `:run` — the complete initial run value; its `checkpoint_seq` is at least
    1 and its `event_seq` equals the number of assigned events.
  * `:checkpoint_type` — must be `:run_initialized`.
  * `:wake_at` — the run's first explicit schedule.
  """
  @type init_proposal :: %{
          required(:transition_id) => transition_id(),
          required(:run) => Docket.Run.t(),
          required(:checkpoint_type) => Docket.Checkpoint.type(),
          required(:wake_at) => DateTime.t()
        }

  @typedoc """
  Data-only claim-fenced transition proposal.

  * `:transition_id` — stable replay identity for this transition.
  * `:run` — the complete next run value; its `checkpoint_seq` must equal
    `expected_checkpoint_seq + 1`.
  * `:expected_checkpoint_seq` — the committed checkpoint sequence this
    transition fences against.
  * `:expected_event_seq` — the run's committed event sequence before this
    transition; the proposal's events extend it contiguously.
  * `:claim_token` — the non-empty claim token that must still hold the run.
  * `:checkpoint_type` — the checkpoint type recorded for this transition.
  * `:schedule` — the claim disposition. `:retain_claim` requires a `:running`
    run; `{:release_claim, :immediate}` and `{:release_claim, {:at, at}}`
    require `:running`; `{:release_claim, :external}` requires `:waiting`;
    `{:release_claim, :terminal}` requires `:done`, `:failed`, or
    `:cancelled`.
  """
  @type claimed_proposal :: %{
          required(:transition_id) => transition_id(),
          required(:run) => Docket.Run.t(),
          required(:expected_checkpoint_seq) => non_neg_integer(),
          required(:expected_event_seq) => non_neg_integer(),
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
          required(:transition_id) => transition_id(),
          required(:run) => Docket.Run.t(),
          required(:expected_event_seq) => non_neg_integer(),
          required(:checkpoint_type) => Docket.Checkpoint.type(),
          required(:schedule) => schedule()
        }

  @type error_reason ::
          :not_found
          | :invalid_transition
          | :stale_checkpoint
          | :conflict
          | :event_conflict
          | :too_large
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
  `{:error, :stale_checkpoint}`. Mutation evaluation must therefore be deterministic,
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

  @doc "Maximum `transition_id` size accepted by every version-1 backend."
  @spec max_transition_id_bytes() :: pos_integer()
  def max_transition_id_bytes, do: @max_transition_id_bytes

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
    if byte_size(transition_id) <= @max_transition_id_bytes and String.valid?(transition_id) do
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
    else
      {:error, :invalid_transition}
    end
  end

  def validate_limits(_proposal, _events, _limits), do: {:error, :invalid_transition}

  @doc false
  @spec validate(
          :initialize | :claimed | :unclaimed,
          non_neg_integer(),
          map(),
          [Docket.Event.t()],
          limits()
        ) :: :ok | {:error, :invalid_transition | :too_large | :event_conflict}
  def validate(kind, expected_checkpoint_seq, proposal, events, limits)
      when kind in [:initialize, :claimed, :unclaimed] and
             is_integer(expected_checkpoint_seq) and expected_checkpoint_seq >= 0 do
    with :ok <- validate_limits(proposal, events, limits),
         :ok <- validate_proposal(kind, expected_checkpoint_seq, proposal),
         :ok <- validate_events(kind, proposal, events) do
      :ok
    end
  end

  def validate(_kind, _expected_checkpoint_seq, _proposal, _events, _limits),
    do: {:error, :invalid_transition}

  @doc false
  @spec transition_id(:initialize | :claimed | :unclaimed, Docket.Run.t()) :: transition_id()
  def transition_id(kind, %Docket.Run{id: run_id, checkpoint_seq: checkpoint_seq})
      when kind in [:initialize, :claimed, :unclaimed] do
    "docket:v1:#{kind}:#{run_id}:#{checkpoint_seq}"
  end

  @doc false
  @spec unique_transition_id(:unclaimed, String.t()) :: transition_id()
  def unique_transition_id(:unclaimed, run_id) when is_binary(run_id) and byte_size(run_id) > 0 do
    suffix = 18 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
    "docket:v1:unclaimed:#{run_id}:#{suffix}"
  end

  @doc false
  @spec digest(:initialize | :claimed | :unclaimed, non_neg_integer(), map(), [
          Docket.Event.t()
        ]) :: binary()
  def digest(kind, expected_checkpoint_seq, proposal, events)
      when kind in [:initialize, :claimed, :unclaimed] do
    :crypto.hash(
      :sha256,
      :erlang.term_to_binary(
        {:docket_transition, @version, kind, expected_checkpoint_seq, proposal,
         digest_events(proposal, events)},
        [:deterministic, minor_version: 2]
      )
    )
  end

  defp digest_events(%{run: %Docket.Run{id: run_id}}, events) when is_list(events) do
    case canonical_events(events, run_id) do
      {:ok, by_sequence} -> by_sequence |> Enum.sort() |> Enum.map(&elem(&1, 1))
      {:error, _reason} -> events
    end
  end

  defp digest_events(_proposal, events), do: events

  @doc false
  @spec encode_result(Docket.Run.t()) :: binary()
  def encode_result(%Docket.Run{} = run) do
    Docket.DurableCodec.encode!(:run, Map.from_struct(run))
  end

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
           expected_event_seq: expected_event_seq,
           claim_token: claim_token,
           checkpoint_type: checkpoint_type,
           schedule: schedule
         }
       )
       when is_integer(expected_event_seq) and expected_event_seq >= 0 and
              is_binary(claim_token) and byte_size(claim_token) > 0 do
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
           expected_event_seq: expected_event_seq,
           checkpoint_type: checkpoint_type,
           schedule: schedule
         }
       )
       when is_integer(expected_event_seq) and expected_event_seq >= 0 do
    if run.checkpoint_seq == expected_checkpoint_seq + 1 and
         checkpoint_type in Docket.Checkpoint.types() and
         valid_schedule?(schedule, run.status, false) and
         Docket.Run.validate_durable(run) == :ok,
       do: :ok,
       else: {:error, :invalid_transition}
  end

  defp validate_proposal(_kind, _expected_checkpoint_seq, _proposal),
    do: {:error, :invalid_transition}

  defp validate_events(kind, %{run: %Docket.Run{} = run} = proposal, events) do
    expected_event_seq =
      case kind do
        :initialize -> 0
        _ -> proposal.expected_event_seq
      end

    with {:ok, by_sequence} <- canonical_events(events, run.id),
         sequences = by_sequence |> Map.keys() |> Enum.sort(),
         true <- run.event_seq == expected_event_seq + map_size(by_sequence),
         true <- sequences == contiguous_sequences(expected_event_seq, run.event_seq) do
      :ok
    else
      {:error, reason} -> {:error, reason}
      false -> {:error, :invalid_transition}
    end
  end

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

  defp contiguous_sequences(first, last) when first == last, do: []
  defp contiguous_sequences(first, last), do: Enum.to_list((first + 1)..last)

  defp valid_schedule?(:retain_claim, :running, true), do: true
  defp valid_schedule?({:release_claim, :immediate}, :running, _claimed), do: true
  defp valid_schedule?({:release_claim, {:at, %DateTime{}}}, :running, _claimed), do: true
  defp valid_schedule?({:release_claim, :external}, :waiting, _claimed), do: true

  defp valid_schedule?({:release_claim, :terminal}, status, _claimed),
    do: status in [:done, :failed, :cancelled]

  defp valid_schedule?(_schedule, _status, _claimed), do: false

  defp encoded_size(term) do
    term
    |> :erlang.term_to_binary([:deterministic, minor_version: 2])
    |> byte_size()
  end
end
