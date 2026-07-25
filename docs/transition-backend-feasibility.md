# Transition backend feasibility

`Docket.Backend.TransitionStore` defines logical atomicity: one transition
receipt, run/checkpoint, schedule/supporting state, and canonical event set
become visible together or none do. A backend capability declaration must
separately describe when an acknowledged commit survives process restart,
machine loss, and failover. Passing the shared suite proves the logical
contract; it does not strengthen a substrate's configured durability policy.

Every implementation persists receipts independently of retained events. The
receipt key is `transition_id`; its value binds owner scope, operation,
checkpoint/event fences, canonical proposal/event digest, and committed result.
Receipts are not subject to event-retention pruning. Exact receipt replay
returns success, while same-scope reuse with different content returns
`:conflict`. A wrong-scope receipt is concealed as `:not_found`.

## SQLite

A SQLite implementation can serialize each operation with `BEGIN IMMEDIATE`,
perform scoped graph/run validation, reserve the receipt row, update the
aggregate, and insert canonical events before one commit. Its capability must
declare:

- journal mode and whether WAL is required;
- `synchronous` policy and the resulting acknowledged-durability boundary;
- busy timeout/backoff policy, with exhausted lock contention normalized to
  `{:retryable, :busy}`;
- that one transition never spans attached databases;
- reopen/recovery tests proving committed receipts and aggregate state agree
  after process termination.

The portable limits are checked before beginning the write. Constraint failures
are permanent contract errors; busy/locked failures are retryable only with the
same transition ID.

## Redis and Redis Cluster

A Redis implementation needs one server-side Function or Lua script that
validates the scoped graph, checks or reserves the durable receipt, applies the
run/schedule state, and publishes canonical events atomically. Optimistic
client-side pipelines are insufficient.

Every key touched by one transition shares a cluster hash tag derived from the
owner/run aggregate, including graph-existence authority and the receipt. If
the backend cannot guarantee that co-slotting, its capability declaration
rejects Redis Cluster rather than weakening atomic graph validation.

The capability also declares persistence and acknowledgement policy (AOF/RDB
and any replica acknowledgement), failover behavior, function deployment
requirements, and how redirection/loading/availability failures map to typed
retryable results. Receipts never inherit event TTLs.

## DynamoDB

A DynamoDB implementation maps a transition to one conditional transactional
write containing the receipt, run/checkpoint, schedule/supporting items, and
event items. Conditional expressions enforce owner scope, immutable identity,
claim token, checkpoint sequence, and event sequence. The receipt item stores
the canonical digest and committed result and remains after event expiry.

Before sending the transaction, the backend enforces both the portable limits
and any stricter current item/action/transaction limits it advertises.
Transaction cancellation reasons normalize deterministically:

- failed owner/resource conditions become `:not_found`;
- immutable or malformed conditions become `:invalid_transition`;
- stale checkpoint/event conditions become `:stale_checkpoint`;
- receipt digest reuse becomes `:conflict`;
- canonical event mismatch becomes `:event_conflict`;
- capacity/throttling/transient service failures become typed retryable
  results;
- non-retryable service/configuration failures become typed permanent results.

Automatic retries are safe only with the identical transition ID and content.
Restart/failover tests verify receipt replay after an ambiguous client outcome.
