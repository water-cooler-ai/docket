# Docket Future Roadmap

Status: living planning document.

This is the general home for future Docket work that is worth preserving but
is not part of a committed release. It may track features, API changes,
correctness work, operational improvements, performance investigations,
developer experience, documentation, research, or experiments.

An entry here is not a release commitment and does not change current product
behavior. Released behavior remains defined by code, module documentation, and
the focused documents under `docs/architecture/`.

## How to use this roadmap

Each idea should record enough context to remain useful without depending on an
external planning system:

- **Status:** idea, exploring, planned, in progress, shipped, or dropped.
- **Horizon:** next, later, or a named release when one exists.
- **Area:** the part of Docket affected.
- **Summary:** what could change.
- **Why:** the user, operator, or maintainer problem it addresses.
- **Possible direction:** the current design shape, without treating it as
  final.
- **Open questions:** decisions or evidence still needed.
- **Dependencies:** work or decisions that should happen first.

Add ideas when they are concrete enough to explain. Split an entry when its
parts can ship or be rejected independently. When work is assigned to a
release, link its focused plan from here. When it ships, move authoritative
details into the relevant contract or guide and reduce the roadmap entry to a
short outcome note.

This roadmap can cover any project area, including:

- runtime and execution;
- graph authoring and public APIs;
- scheduling and multi-tenancy;
- persistence, migrations, and backend conformance;
- operations, observability, and recovery;
- performance and scalability;
- developer experience, testing, and documentation; and
- research or experimental capabilities.

## Future work

### Scheduling and multi-tenancy

#### Preferred tenant share, borrowing, and reclaim

- **Status:** exploring
- **Horizon:** later
- **Area:** PostgreSQL TenantFair admission

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
- **Area:** PostgreSQL TenantFair scheduling and service accounting

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

- [v0.1.1 composability and ergonomics roadmap](roadmap-v0.1.1.md) tracks work
  already organized around that release theme.

## Entry template

```markdown
### Area

#### Idea name

- **Status:** idea | exploring | planned | in progress | shipped | dropped
- **Horizon:** next | later | release name
- **Area:** affected subsystem

**Summary:** What could change.

**Why:** The problem or opportunity.

**Possible direction:** Current design thoughts, if any.

**Open questions:** Decisions, risks, and evidence still needed.

**Dependencies:** Work or decisions that should happen first.
```
