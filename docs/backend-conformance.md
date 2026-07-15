# Backend conformance

`Docket.Backend.Conformance` is Docket's reusable ExUnit profile for backend
authors. The memory and PostgreSQL backends run the same generated cases. A
third-party backend can run them from its own test environment without Ecto,
Postgrex, or access to Docket's `test/support` modules.

## Harness contract

Implement `Docket.Backend.Conformance.Harness`. The harness owns only
substrate lifecycle and isolation:

- start shared services or apply migrations in optional `setup_suite/0`;
- start, reset, or isolate one backend in `setup_case/2`;
- return the backend module, its opaque root context, a unique namespace, and
  a UTC test time in a `Docket.Backend.Conformance.Instance`;
- release resources in optional teardown callbacks.

The harness must not return focused stores or seed Docket fixtures. The shared
profile derives stores exclusively from `backend.graphs/0`, `backend.runs/0`,
and `backend.events/0`, and it owns every graph, run, event, scope, claim
policy, and expected result.

A minimal process-backed harness looks like this:

```elixir
defmodule MyApp.BackendConformanceHarness do
  @behaviour Docket.Backend.Conformance.Harness

  alias Docket.Backend.Conformance.Instance

  @impl true
  def setup_case(_suite_state, _ex_unit_context) do
    name = Module.concat(__MODULE__, "Case#{System.unique_integer([:positive])}")
    ExUnit.Callbacks.start_supervised!({MyApp.Backend, name: name})

    {:ok,
     %Instance{
       backend: MyApp.Backend,
       context: MyApp.Backend.context(name: name),
       namespace: "case-#{System.unique_integer([:positive, :monotonic])}",
       now: DateTime.utc_now() |> DateTime.truncate(:microsecond)
     }}
  end
end
```

Then invoke the Docket-owned cases unchanged:

```elixir
defmodule MyApp.BackendConformanceTest do
  use ExUnit.Case, async: false

  use Docket.Backend.Conformance,
    harness: MyApp.BackendConformanceHarness
end
```

`setup_suite/0` defaults to `{:ok, nil}`. When implemented, it must return
`{:ok, suite_state}`; that state is passed to every `setup_case/2` call.
`setup_case/2` must return `{:ok, %Instance{}}`. Optional
`teardown_case/1` and `teardown_suite/1` callbacks run through ExUnit
`on_exit`, including after failures.

## What passing demonstrates

Each failure includes a stable invariant ID. The profile checks:

- all mandatory `Docket.Backend`, `GraphStore`, `RunStore`, and `EventStore`
  callbacks, with `context/1` treated explicitly as optional;
- transaction commit, error rollback, exception/throw propagation, invalid
  return handling, nested participation, rollback-only propagation, concurrent
  publication, and pre-commit visibility;
- graph/run/event compatibility and atomicity through one yielded context;
- explicit graph ownership and run/event tenant scope isolation;
- graph content addressing, idempotence, latest/version reads, and owner
  isolation;
- event idempotence, conflict and mismatch rejection, point/latest/page reads,
  cursors, ordering, and sparse retained histories;
- claim fencing, exact checkpoint sequencing, refresh/release authority,
  abandonment, poisoning, recovery, same-fence concurrency, and serialized
  mutation safety.

The yielded context is opaque and only guaranteed for transactional
participation inside the callback. The profile does not require value identity
between nested callbacks or runtime invalidation after a callback returns.

## Deliberate implementation-specific coverage

Some guarantees cannot be induced through the public store contract. Keep
these in backend-specific suites:

- physical pruning and fully corrupted retained histories;
- raw stored-content corruption and the impossible-without-a-hash-collision
  graph conflict branch;
- SQL locks, query plans, prefixes, migrations, schemas, and notifications;
- Agent snapshot/lock mechanics and deterministic substrate race hooks;
- performance and failure injection.

Core lifecycle integration tests separately verify the semantic event sets and
checkpoint proposals constructed by Docket itself. The backend profile verifies
that a backend stores those proposed run/event transitions atomically; it does
not reimplement the lifecycle planner.
