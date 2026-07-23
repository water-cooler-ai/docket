# Docket Reducers — 0.1 Contract Rationale

The 0.1 contract reduces against the prior committed value so aggregate fields
can accumulate across steps.

## Reduction contract

Aggregates such as chat histories, usage counters, and merged metadata need the
reducer to fold the **prior committed value** as well as the current step's
writes:

    new_value = reduce(reducer, current_committed_value, sorted_step_writes)

`last_value` is the degenerate case that ignores `current`, preserving ordinary
replacement semantics. Built-in reducer types, options, and shorthand remain
descriptor data on the wire.

## Why the runtime owns reduction

Reduction lives in `Docket.Reducer.reduce/3`, a pure function called from
the update barrier (`apply_state_writes`) and from interrupt resolution.
Reducers are part of the determinism contract: replanning after a crash must
reproduce identical commits, which is why 0.1 ships only built-in reducers
and defers module-referenced custom reducers until a real host graph
exhausts the built-ins (every custom reducer hands the purity obligation to
the host and makes a module name durable graph content).

## Decisions and their reasons

- **List writes concatenate** (`append`/`union`). LangGraph's convention:
  a list write contributes elements, a scalar write contributes one element.
  This is type-ambiguous only when the field's `item` is itself a list type,
  so the compiler warns (`:ambiguous_list_write`) there instead of the
  contract forbidding list writes everywhere.
- **Natural zeros as effective defaults.** Accumulating fields get `[]` /
  `%{}` / `0` as the channel default during lowering when the field declares
  none. Rationale: nodes and guards should see an empty aggregate, not
  missing state, and the first reduction should fold into the zero — this
  keeps "first write" and "later writes" on one code path. An explicit field
  default acts as the base the first commit folds into, which is also why
  `first_value` treats an explicit default as the value already being set.
- **Write validation is reducer-aware, the field schema stays committed
  truth.** A node writing to an `append` field writes an *item*; validating
  the item against the field's list schema would always fail. Each reducer
  defines what one write validates against (`Docket.Reducer.write_schema/3`)
  while the field schema keeps describing the committed shape. Committed
  values are not re-validated after reduction — the compiler's
  reducer/schema pairing diagnostics (`append`/`union` ⇒ list, `sum` ⇒
  numeric, `merge` ⇒ map/object) guarantee the reduction preserves the
  committed type.
- **Interrupt resolutions flow through the reducer.** Resolution already
  wrote through `apply_state_writes`, so resolving into an `append`
  field accumulates with no special case. This fell out of the v0.1 design
  rather than being added.
- **Union dedupes first-occurrence-wins.** `union` is list-as-set:
  membership semantics, not upsert. A keyed replace-in-place reducer can be
  added later without changing `union`.

## Wire compatibility

Reducers serialize as `type` plus open `opts`; built-in types and options remain
additive descriptor data.
