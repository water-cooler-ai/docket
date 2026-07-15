# Delivery and Execution Guarantees

This document defines the current delivery and execution contract of Docket's
durable PostgreSQL runtime. The short version is:

> Docket atomically commits one winning durable transition. The node attempts
> that propose a transition may execute more than once. External effects are
> effectively-once only when the receiving system atomically deduplicates a
> stable Docket idempotency identity. Observational callbacks are best effort.

These guarantees apply at different boundaries and must not be collapsed into
a single "exactly-once" or "at-least-once" label.

## Guarantee matrix

| Boundary | Current guarantee | Source of truth | Required consumer behavior |
| --- | --- | --- | --- |
| Run transition and its retained events | Atomic, single durable winner | PostgreSQL transaction containing the fenced run update and event append | None inside Docket's storage boundary |
| Claim ownership | One current commit authority, not one executor | Current `claim_token` plus expected `checkpoint_seq` | Treat a lease as revocable authority |
| Node attempt execution | Replayable; the same attempt may execute zero, one, or multiple times | Last committed run state | Keep node computation pure or make effects idempotent |
| External effect initiated by a node | May happen more than once or have an ambiguous outcome | The external system | Atomically deduplicate the supplied identity with the effect |
| Retained event persistence | Atomic with its run transition and idempotent by `{run_id, seq}` | `docket_events` during its configured retention period | Do not confuse persistence with delivery |
| Retained event export or consumption | No built-in delivery guarantee | Application exporter and consumer state | Use durable cursors, retry, and deduplicate by `{run_id, seq}` |
| `checkpoint_observers` and poison callbacks | Best effort; may be lost or duplicated | Durable run and event rows, not the callback | Use only for hints and observability |
| LISTEN/NOTIFY wakeup | Best effort latency optimization | Polling the durable run queue | Never rely on a notification for correctness |
| Telemetry | Best effort operational signal | Durable run and event rows | Never use telemetry as a business event log |

## Exact boundary of the durable guarantee

One runtime moment proposes a next `Docket.Run`, assigned events, checkpoint
metadata, and a scheduling disposition. Docket commits that proposal in one
backend transaction. The run update is accepted only when both of these match:

```text
stored claim_token    = vehicle claim_token
stored checkpoint_seq = expected checkpoint_seq
```

The proposed run must advance `checkpoint_seq` by exactly one. The run update,
schedule change, and retained event append either all commit or all roll back.
A stale vehicle may have executed node code, but it cannot commit after another
claimant changes the token or advances the sequence.

The guarantee ends at transaction commit. Work performed before that commit,
including network calls made by node code, is outside the transaction and
cannot be retracted when a fence is lost. Work performed after commit,
including observers, notifications, and telemetry, cannot veto the committed
transition and is not durably delivered by Docket.

## What "at least once" means here

Docket documentation uses **replayable execution** instead of an unqualified
"at-least-once execution" promise. At-least-once is useful shorthand for the
duplicate-risk boundary, but it can incorrectly imply that every intended
effect is eventually performed.

Docket does not make that unconditional liveness guarantee. A node attempt can
happen zero times because a run is cancelled, fails validation, becomes
poisoned, exhausts retry policy, or never reaches a compatible healthy worker.
Subject to an available durable backend, compatible workers, applicable retry
and poison recovery policy, and no cancellation, Docket keeps eligible durable
work recoverable. Once execution begins, an uncommitted attempt may be replayed
after a crash, timeout, claim steal, or ambiguous commit result.

## External effects and effective-once processing

Docket derives task identity from committed state. Replanning an uncommitted
attempt produces the same `task_id` and `idempotency_key`; advancing an explicit
node retry produces a new attempt identity. A cooperating integration can use
that identity to turn replayable execution into effectively-once processing:

1. Receive the Docket idempotency identity with the request.
2. In the same transaction as the external effect, insert or check that
   identity in a uniqueness-enforced ledger.
3. Store the effect's result with the identity.
4. Return the stored result when the same identity is received again.

Checking a key and performing an effect in separate transactions leaves the
same crash window and is not sufficient. Whether deduplication should use an
attempt-level `idempotency_key`, logical `task_id`, or a domain identity such as
an order ID is an application decision:

- `idempotency_key` permits an explicit Docket retry attempt to try the effect
  again while deduplicating crash replay of that attempt.
- `task_id` deduplicates all attempts for one node activation.
- A domain identity can deduplicate across runs when the business operation
  itself must be unique.

Docket cannot enforce exactly-once effects against an arbitrary external API
that does not participate in this protocol. Holding a database transaction
open across node execution would not solve that problem and would weaken
availability and recovery.

## Consistency and partition behavior

PostgreSQL is Docket's coordination authority. During a partition, a worker
that cannot reach the authoritative database cannot safely advance durable
state. Another worker may recover an expired claim, execute the same attempt,
and become the sole durable winner. Docket therefore chooses consistent run
state over continued writes by a partitioned worker.

This does not make external effects single-winner. A partitioned or stale
worker may already have performed an effect before losing commit authority.
The external idempotency protocol remains necessary.

Database replication, failover durability, and read routing must preserve the
committed PostgreSQL history on which Docket relies. Promotion of a replica
that has not received acknowledged commits can invalidate guarantees above
the single database history visible to Docket.

## Integration rules

- Put business-critical effects in node integrations that use stable
  idempotency, or park the run and let an external durable system complete the
  work before resolving it through a signal.
- Use retained events as the durable export source. Advance a durable consumer
  cursor only after downstream acceptance, and make downstream processing
  idempotent by `{run_id, seq}`.
- Size event retention so the slowest supported consumer cannot fall behind
  the oldest retained event. Retention expiry is an intentional loss boundary.
- Use observers, notifications, and telemetry only for caches, UI hints,
  wakeups, diagnostics, and other reconstructable projections.
- Treat timeout and cancellation as loss of local execution control, not proof
  that an external effect did not happen.

