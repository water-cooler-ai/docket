# Docket Future Roadmap

Items are exploratory, not release commitments. Current behavior is defined by
the code, module documentation, and focused architecture and operations guides.

## Future work

### Scheduling and multi-tenancy

#### Preferred tenant share, borrowing, and reclaim

- **Status:** exploring
- **Horizon:** later
- **Area:** PostgreSQL claim-policy admission (builds on reintroduced
  per-tenant caps, which are not part of v0.1.0)

**Summary:** Separate a tenant's preferred concurrency from its hard maximum.
Allow tenants to borrow otherwise-idle capacity while giving newly backlogged
tenants priority as capacity becomes available again.

**Why:** A single hard maximum forces operators to choose between conservative
tenant isolation and high utilization. Preferred share plus borrowing could
use quiet capacity without discarding meaningful tenant entitlements.

**Possible direction:**

- Keep `preferred_active` and `max_active` independent.
- Treat preferred capacity as a steady-state target rather than a permanently
  reserved or preemptible slot count.
- Admit entitled work before borrowed work.
- Allow borrowing only up to the borrower's exact hard cap.
- Return capacity non-preemptively as active borrowed runs release admission.
- Describe reclaim using qualifying admission and release opportunities unless
  a wall-clock bound can be proven honestly.
- Add aggregate observations for entitled, borrowed, and reclaim-wait states.

**Open questions:**

- What capacity domain should preferred shares divide?
- How should fleet-wide demand interact with per-tenant sticky admission when
  every vehicle slot is occupied?
- What is the smallest useful reclaim promise for operators?
- How should class ordering remain bounded under contention?
- Which administration and migration changes are justified by real usage?

**Dependencies:** Preserve exact per-owner caps, non-preemptive cap debt,
bounded candidate discovery, rollback neutrality, and the existing
ClaimPolicy/RunStore boundary.

#### Active-set weighted tenant service

- **Status:** idea
- **Horizon:** later
- **Area:** PostgreSQL claim-policy scheduling and service accounting

**Summary:** Distribute service proportionally among currently active tenants
using configured integer weights.

**Why:** Equal rotation opportunities do not express differentiated service
tiers and do not account for tenants whose work consumes different amounts of
concurrency or processing time.

**Possible direction:**

- Maintain normalized service tags for the active tenant set.
- Introduce a system virtual-time floor and clamp returning tenants to it so
  idle tenants cannot accumulate stale credit.
- Derive service asynchronously from claim and completion evidence.
- Keep lifecycle commit, release, refresh, and abandon paths free of new
  opposite-order partition locks.
- Consider concurrency share and processing-time share as complementary
  signals.
- Define accounting staleness, reconciliation, and long-run skew measurements.

**Open questions:**

- What is the authoritative service unit?
- How is the active set defined across joins, leaves, waits, and policy
  changes?
- Should weights influence admission, observation, or both?
- What tolerance and observation window make a proportionality claim useful?
- How should missing, duplicated, delayed, or replayed accounting evidence be
  reconciled?

**Dependencies:** Settle the normalized-service and entitlement model first.
Asynchronous accounting must remain independent of exact-cap correctness and
must not introduce partition/run lock cycles.

## Related focused roadmaps

- [Composability and ergonomics roadmap](composability-roadmap.md) tracks work
  around graph composition, authoring, and detached execution.
