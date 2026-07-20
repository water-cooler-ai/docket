# Backend tests

`Docket.BackendTests` is Docket's shared black-box test suite for backend
implementations. It lives under `test/support`, not in the shipped library API.
The production contract is defined by `Docket.Backend`,
`Docket.Backend.GraphStore`, `Docket.Backend.RunStore`, and
`Docket.Backend.EventStore`. Backends may additionally expose the narrow
`Docket.Backend.ClaimPolicyAdmin` capability through the optional
`claim_policy_admin/0` accessor.

## Docket-owned backends

Docket keeps one explicit backend matrix in
`test/docket/backend_tests_test.exs`. A `for` comprehension generates a test
module for each backend and applies the same `Docket.BackendTests` cases.
Backend-specific setup remains ordinary ExUnit setup code under
`test/support/backend_test_setup`.

Adding an in-repository backend therefore requires two things:

1. an ordinary setup module that starts or resets its substrate and returns a
   test subject;
2. one entry in the backend matrix.

The shared suite constructs every graph, run, event, scope, claim policy, and
expected result. Setup owns only substrate lifecycle and isolation.

## External backends

External backend projects should run the shared tests from a Docket source
checkout pinned to the same release tag they support. Do not test a released
backend against Docket's moving default branch.

Load the shared support files from the checkout in `test/test_helper.exs`:

```elixir
docket_source = System.fetch_env!("DOCKET_SOURCE_PATH")

for file <- [
      "test/support/backend_tests.ex",
      "test/support/backend_tests/contract.ex",
      "test/support/backend_tests/fixture.ex",
      "test/support/backend_tests/cases.ex"
    ] do
  Code.require_file(Path.join(docket_source, file))
end

ExUnit.start()
```

Then provide normal ExUnit setup and use the cases:

```elixir
defmodule MyBackend.SharedBackendTest do
  use ExUnit.Case, async: false

  setup do
    context = start_or_reset_backend!()
    on_exit(fn -> stop_backend(context) end)

    {:ok,
     backend_test: %{
       backend: MyBackend,
       context: context,
       namespace: "case-#{System.unique_integer([:positive, :monotonic])}",
       now: DateTime.utc_now() |> DateTime.truncate(:microsecond)
     }}
  end

  use Docket.BackendTests
end
```

The subject map contains the backend module, its opaque root context, a unique
namespace, and a deterministic microsecond-precision UTC timestamp. The suite
resolves focused stores exclusively through the returned backend.

## What passing demonstrates

Each failure includes a stable invariant ID. The shared cases currently cover:

- mandatory backend and focused-store callbacks;
- commit, rollback, nested participation, rollback-only propagation,
  concurrent publication outcomes, and completed-read visibility;
- graph/run/event compatibility and atomicity through one yielded context;
- graph ownership and run/event tenant isolation;
- graph content addressing, idempotence, latest/version reads, and pagination;
- event idempotence, conflict and mismatch rejection, ordering, cursors, and
  sparse retained histories;
- claim fencing, exact checkpoint sequencing, refresh/release authority,
  abandonment, poisoning, recovery, same-fence outcomes, and serialized
  mutation safety.

Passing is evidence for the portable cases above, not a claim that every
backend is operationally equivalent. In particular, the shared suite does not
currently establish the complete `claim_due` selection matrix, all run-list
filters, restart durability, migrations, substrate configuration, deterministic
lock timing, or end-to-end runtime advancement.

## Backend-specific coverage

Keep implementation and operational guarantees with the backend that owns
them. Examples include:

- restart/reopen durability and independent-process access;
- migrations, schemas, prefixes, SQLite pragmas, and PostgreSQL notifications;
- physical pruning, deliberately corrupted rows, and query plans;
- deterministic lock/contention hooks, performance, and failure injection;
- starting the real backend supervision tree and proving due work advances.

Portable omissions should be promoted into `Docket.BackendTests` so every
backend receives the same assertion. Substrate mechanics should remain in
backend-specific suites.

## PostgreSQL ClaimPolicy implementations

Claim-policy administration is a portable optional capability even though the
PostgreSQL ClaimPolicy execution engine below is a backend-private extension
seam. A capable backend implements all five `Docket.Backend.ClaimPolicyAdmin`
callbacks and returns that module from `claim_policy_admin/0`; incapable
backends omit the accessor. This explicit accessor controls configured-module
facade generation and never depends on whether PostgreSQL modules are loaded.

ClaimPolicy is a PostgreSQL-backend extension seam rather than a portable
`Docket.Backend` capability. Its source-owned reusable cases live in
`test/support/claim_policy_tests.ex` and
`test/support/claim_policy_run_store_tests.ex`. Implementations are registered
once in `test/support/claim_policy_matrix.ex`, and both the focused ClaimPolicy
suite and live RunStore suite consume that registry so an implementation cannot
silently omit either contract. The pure contract verifies policy construction and
selected-implementation binding, invalid runtime inputs, one data-only
statement, suite-owned decoded-batch semantics, decoder error normalization,
the alternate implementation's bounded data-only policy-error variant,
rejection of its non-data reason, bounded observations, and generic
success/error telemetry metadata. Each
implementation fixture only encodes the suite-owned batch as its row format
and identifies input that its decoder must reject. The RunStore contract uses
live PostgreSQL to verify one selected plan query, decoded batch return, and
unchanged query-error return for the same implementations except the explicit,
backend-wide SQLSTATE `25006` normalization to the portable read-only
transaction error.

The shared matrix complements direct RunStore, transaction, supervised
dispatcher, manual drain, PostgreSQL claim, fencing, poison, telemetry,
query-plan, and contention tests. Implementations build and decode plans; they
never call RunStore admission. The matrix and its fixtures compile only from
`test/support` in the test environment. The package allowlist in `mix.exs`
excludes `test`, so none of these modules are part of the Hex runtime API.
