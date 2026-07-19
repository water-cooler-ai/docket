# TenantFair schema-v3 sticky admission

Schema V3 changes TenantFair's per-owner cap from transient claim tokens to
sticky logical-run admission. The V2 unfinished-tenant ring and its bounded
cross-tenant traversal remain the outer scheduler.

## Derived states

`docket_runs.tenant_admitted_at` is an internal nullable timestamp. It does not
add a public run status.

- **queued:** healthy `running`, due, unclaimed, and unadmitted;
- **admitted-ready:** healthy `running`, due, unclaimed, and admitted;
- **admitted-claimed:** healthy `running`, claimed, and admitted; and
- **inactive:** future-scheduled, externally waiting, poisoned, or terminal,
  with no admission.

The normal-state invariant is:

```text
live_claim_count(scope) <= admitted_run_count(scope) <= max_active_runs(scope)
```

A cap decrease may create debt, so existing admissions are not preempted. The
engine promotes no queued work while admitted count is at or above the cap.

## Admission and FIFO

Promotion is the only transition that creates an admission. Under the existing
tenant-partition lock, the claim function freshly counts admitted healthy rows
and promotes only while capacity remains. Promotion order is `(wake_at,
internal id)` among due, healthy, unclaimed, unadmitted rows.

Already-admitted eligible work is served before promotion. A queued candidate
may be promoted only from a contiguous locked and rechecked FIFO prefix; a
locked or stale head may underfill a visit but cannot be bypassed. Admitted
work may continue to rotate by ready/expired ordering after promotion.

Two permanently runnable admitted runs at cap two intentionally keep later
runs queued forever. Cross-tenant ring fairness does not imply within-tenant
time slicing beyond the admitted cohort.

## Admission lifetime

Cooperative drain yield, generic immediate claim release, refresh,
reacquisition, vehicle replacement, and expired steal preserve the original
admission timestamp. Future scheduling, external waiting, terminal completion,
failure, cancellation, host-incompatible abandon/backoff, and poison clear it.
Waking a previously unadmitted run leaves it queued.

Every transition is atomic with lifecycle state and retains the existing claim
token and checkpoint-sequence fences. A stale holder cannot clear a newer
admission, and transaction rollback persists neither side.

## Schema and indexes

V3 adds nullable `tenant_admitted_at timestamptz`, constrains a non-null marker
to a healthy `running` row, and adds partial indexes for admitted count,
admitted-ready order, queued-ready order, and admitted expired-claim order.
The generic V2 ready/live/expired indexes remain available to Legacy.

The stopped migration backfills every healthy claimed row from `claimed_at`.
Unclaimed rows remain queued. It does not trim an over-cap tenant; those rows
become admission debt. The prefix-qualified unversioned claim function is
replaced in the same migration transaction.

## Administration and observability

The public configuration name is `default_max_active_runs`; Admin values use
`max_active_runs`. Database columns named `max_active` remain internal.
`Admin.get_effective/2` returns effective cap and versions plus token-free
aggregate `queued`, `admitted_ready`, `admitted_claimed`, and `debt` counts.
Metrics and ordinary trace labels never contain tenant, run, or claim-token
identity.

## Rollout

The supported upgrade is stopped and homogeneous:

1. stop dispatchers and all Docket run writers;
2. deploy lifecycle and admission code together;
3. apply V3 and its backfill; and
4. restart directly with one binary and one selected engine.

Generate `--upgrade-from-v2` for a landed V2 installation. A generated
`--upgrade-from-v1` migration applies V2 and V3 together and reverses both on
rollback. V3 downgrade removes the marker and V3 indexes and recreates the V2
claim function. Fresh migration, upgrade, downgrade, tenantless ownership, and
custom-prefix paths are transactional.

The runtime checks the recorded schema version before starting backend
children. Mixed lifecycle writers and online Legacy-to-TenantFair cutover are
not supported protocols; stop the fleet rather than relying on compatibility
aliases or dual writes.
