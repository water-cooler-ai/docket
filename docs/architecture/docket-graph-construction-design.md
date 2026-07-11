# Docket: Graph Construction Public API Design

Status: reference draft
Date: 2026-06-25

Release note: this document records the `0.0.1` host-owned graph persistence
boundary. In `0.1.0`, graph construction and serialization remain public, but
a required backend publishes effective graph versions and `start_run` accepts
their `Docket.GraphRef`. The operational transition spec governs where the
boundaries differ.

Related documents:

- `docs/architecture/docket-runtime-design.md`
- `docs/architecture/docket-compiler-design.md`

Implementation note: this document owns the detailed graph document and
compiler construction decisions; concrete APIs are canonical in the module
docs under `lib/docket/`.

## 1. Executive Summary

This document designs Docket's public graph construction layer.

The key naming decision is:

```text
Docket.Graph is the canonical, user-facing graph.
Docket.Run is the canonical, user-facing run state document.
Docket.Runtime.Graph is the internal executable runtime graph.
```

Applications, workflow compilers, graph editors, and product UIs work with
`Docket.Graph`. It contains fields, nodes, edges, guards, outputs, policies,
metadata, and compiler diagnostics.

The Runtime does not execute `Docket.Graph` directly. When a run starts, Docket
materializes the canonical graph into an internal `Docket.Runtime.Graph` value
with channels, subscriptions, reducers, generated edge channels, barriers, and
runtime node definitions.

Docket does not own graph storage. Host applications save, version, relate, and
load graph documents however they like. Docket accepts graph documents as values,
verifies them, runs them, and emits updated run documents through checkpoints.

```text
draft/edit/save:
  Docket.Graph document -> host-owned storage

publish/verify:
  Docket.Graph.verify/2 -> {:ok, graph} | {:error, graph with diagnostics}
  -> host-owned publish/save

run:
  host loads Docket.Graph document
  -> Docket.run/4 or MyApp.Docket.run/3
  -> internal runtime compile
  -> Docket.Checkpoint emits Docket.Run document
```

`Docket.Runtime.Graph` is a derived implementation detail. Its shape can change
as the runtime improves without changing the public graph model.

## 2. Document Model

There are four related but distinct things:

```text
Docket.Graph
  User-facing graph definition document.
  Canonical.
  Editable while the host treats it as a draft.
  Immutable once the host stores it in a published graph artifact.
  Suitable for UI, workflow compilers, host-owned storage, and inspection.

Docket.Run
  User-facing durable run state document.
  Emitted through Docket.Checkpoint callbacks.
  Saved by the host application.
  Passed back to Docket to resume or retry a run.
  Stores the graph hash captured when the run was created.

Docket.Runtime.Graph
  Internal executable graph materialization.
  Derived from Docket.Graph.
  Consumed by the Runtime.
  Contains channels, runtime node records, generated barriers, and lowering maps.

Run State
  Mutable execution state for one run.
  Owned by one Runtime process.
  Contains channel values, versions, active tasks, interrupts, timers, events,
  and checkpoints.
```

The public construction layer is centered on `Docket.Graph`. The runtime graph
is not the storage format and not the user-facing API. The host stores
`Docket.Graph` and `Docket.Run` documents. A `Docket.Run` is the durable
execution state document; hosts persist and pass it back without interpreting
Docket-owned execution internals.

## 3. Design Goals

1. Make `Docket.Graph` the canonical graph used by applications.
2. Keep graph storage app-owned; Docket accepts and returns graph documents.
3. Keep `Docket.Runtime.Graph` internal and derived.
4. Support realtime graph construction through simple graph update functions.
5. Support app-side graph editors through stable public IDs while keeping UI
   layout outside Docket.
6. Allow incomplete drafts while users edit; attach diagnostics only when the
   graph is verified or compiled.
7. Use compiler verification and compilation as the blocking gate for
   publish/run.
8. Keep published host graph artifacts append-only and immutable.
9. Preserve stable public IDs for graph, node, edge, field, and output records.
10. Compute private SHA-256 graph identity once from the published effective
    graph bytes so Docket can verify compatibility without owning host version
    IDs.
11. Keep generated channels hidden from normal users but inspectable in runtime
    debug tools.

## 4. Non-Goals

1. Do not introduce a separate `StateGraph`, `Builder`, `NodeSpec`, or
   `EdgeSpec` schema that mirrors `Docket.Graph`.
2. Do not ask UI code to construct `Docket.Runtime.Graph.Channel` or
   `Docket.Runtime.Graph.Node` values.
3. Do not mutate a published host graph artifact in place.
4. Do not require every UI edit to produce a valid executable graph.
5. Do not make any UI canvas JSON the canonical graph model.
6. Do not store UI layout, canvas state, viewport state, zoom, positions, or
   editor projection data inside Docket graph documents.
7. Do not require a Docket graph storage behaviour.
8. Do not make compile receipts or runtime graph hashes part of the loop public
   model.

## 5. Module Shape

Recommended public modules:

```text
Docket.Graph
Docket.Graph.Node
Docket.Graph.Edge
Docket.Graph.Field
Docket.Graph.Output
Docket.Graph.Compiler
Docket.Node
Docket.Schema
Docket.Reducer
Docket.Guard
```

`Docket.Guard` is the public constructor module for durable guard expressions
used by graph nodes and edges:

```elixir
Docket.Guard.changed(channel)
Docket.Guard.version_at_least(channel, version)
Docket.Guard.path(channel, path)
Docket.Guard.exists(ref)
Docket.Guard.equals(ref, value)
Docket.Guard.all(expressions)
Docket.Guard.any(expressions)
Docket.Guard.not(expression)
```

Recommended internal/runtime modules:

```text
Docket.Runtime.Graph
Docket.Runtime.Graph.Node
Docket.Runtime.Graph.Channel
Docket.Runtime.Graph.Lowering
Docket.Runtime
```

The naming boundary is:

- `Docket.Graph.Node` is an editable public graph node.
- `Docket.Node` is the executable node behaviour implemented by user code.
- `Docket.Runtime.Graph.Node` is an internal lowered runtime node definition.

When code must reference both public graph nodes and runtime graph nodes in the
same scope, alias `Docket.Runtime.Graph.Node` as `RuntimeNode`.

## 6. Docket.Graph

`Docket.Graph` is the canonical public graph document and the structure UI tools
edit.

It contains:

- graph identity, supplied with `id:` or generated by Docket
- input fields
- state fields
- output projections
- nodes
- edges
- node-local branch groups over outgoing edges
- graph-level policies
- application metadata
- compiler diagnostics

Sketch:

```elixir
defmodule Docket.Graph do
  defstruct [
    :id,
    :name,
    :description,
    :schema_version,
    fields: %{},
    inputs: %{},
    outputs: %{},
    nodes: %{},
    edges: %{},
    policies: %{},
    metadata: %{},
    diagnostics: []
  ]
end
```

This is not a temporary builder. The bang functional API returns updated
`Docket.Graph` values for pipe-oriented construction. The editing API itself
(`new`/`new!`, the `put_*`/`update_*`/`delete_*` helpers, and the accepted
attribute shapes) is documented in the `Docket.Graph` module docs
(`lib/docket/graph.ex`).

### 6.1 Private Effective-Graph Identity

During publication, Docket computes a full SHA-256 digest of the effective
graph's canonical durable bytes. It is a private content identity used by
storage and run compatibility, not a host-owned version ID or a public graph
operation. Hosts may still maintain their own graph version numbers, slugs,
revision IDs, or publishing records.

There is no public `Docket.Graph.hash/1`. Authored graphs are not hashable
publication identities: compiler ingest canonicalizes their durable content,
schema materialization applies defaults, and the final effective graph is
canonicalized once more. Only then does the compiler encode the graph and
compute its private digest from those exact bytes. The same effective graph,
bytes, and digest flow into lowering and storage without a second projection.

Hash contract:

```text
algorithm: "sha256"
format_version: 1
digest: lowercase SHA-256 hex digest
```

The hash input is the exact private versioned deterministic ETF encoding of the
diagnostic-free effective `%Docket.Graph{}`. The stored bytes and hash input are
identical; there is no second canonical digest format. Deterministic ETF byte
stability is scoped to one codec version and OTP major, so incompatible struct
or OTP changes require a codec-version bump and controlled rewrite.

`Docket.Graph.Compiler.Canonical` owns graph-specific normalization and
structural validation. In particular, every recovered graph collection must
be a plain map with binary keys and values of the exact expected graph struct
type. `Docket.DurableCodec` owns only the generic versioned ETF envelope,
deterministic encoding, safe/full decoding, known durable-term validation, and
root validation. Recovery either returns an already-canonical effective graph
or fails closed; it never repairs malformed stored graph structure.

The durable projection excludes compiler diagnostics and includes the public
graph structure and semantics: input/state/output definitions, nodes, node
names or labels, node branch groups, edges, edge guards, policies, and
metadata. Graphs remain free-form while being edited; publication normalizes
atom keys and values in open content to strings and rejects non-portable
resources before encoding the graph directly with the private codec.

The one caveat of boundary coercion: an authored graph may still contain atom
keys before publication, while its effective published graph uses strings.
Host-facing docs should state the rule of thumb: treat open content (config,
metadata, policies, defaults) as string-keyed data on both write and read.

Public node IDs are stable and meaningful in v1, so the durable encoding hashes
node and edge references directly. A node name or label change is considered a
graph content change.

`Docket.Run.graph_hash` stores the private digest captured from the effective
graph at publication. Resume compatibility is:

```text
graph.id == run.graph_id
compiled_effective_graph.graph_hash == run.graph_hash
```

Store the full SHA-256 digest. UI and logs may display a short prefix, but
internal equality checks should use the full hash.

### 6.2 ID Rules

All IDs stored in `Docket.Graph` are binaries in v1.

This includes graph IDs, input IDs, field IDs, output IDs, node IDs, edge IDs,
branch group names, and any graph references that point at those records. Graph
editing helpers accept binary IDs. Non-bang helpers return
`{:error, Docket.Graph.Error.t()}` for atom IDs, charlists, numbers, or other
terms; bang helpers raise `Docket.Graph.Error`. Docket must not create atoms from
graph IDs or from user-supplied strings.

Canonical v1 public IDs are non-empty binaries that match:

```text
~r/^[A-Za-z0-9][A-Za-z0-9_-]*$/
```

The only reserved graph endpoint IDs outside that pattern are the pseudo-node
IDs `"$start"` and `"$finish"`. They may be used as edge endpoints, but they
cannot be used as user node IDs or any other public graph record ID.

If an ID is omitted, Docket generates a binary ID using the configured ID
generator. Default generated public ID prefixes are:

```text
graph_<token>
input_<token>
field_<token>
output_<token>
node_<token>
edge_<token>
```

Tests may inject deterministic ID generation, such as `edge_000001`, but the
canonical stored value is still a binary.

Runtime-generated IDs are also binaries, but they are internal and namespaced by
runtime kind:

```text
input:<input_id>
state:<field_id>
output:<output_id>
edge:<edge_id>
```

Fan-in joins are represented as ordinary public edge records where `from` is a
list of source node IDs. They lower through the same `edge:<edge_id>` runtime
channel family as simple edges, though the runtime channel may use barrier/all
semantics internally.

Branches do not create branch-specific runtime channels in v1. Branch groups
are node-local metadata that name outgoing guarded edge records, and each
branch arm activates through the same `edge:<edge_id>` channel format as any
other edge. The node ID plus branch group name remains useful for editing,
diagnostics, lowering metadata, and UI overlays.

Every `edge:<edge_id>` runtime channel is backed by a canonical edge record with
`id`, `from`, and `to`. Fan-out helpers may create multiple edge records,
fan-in helpers may create one multi-source edge record, and branch helpers may
group outgoing edge IDs on the source node, but the compiler still lowers
activation through edge record IDs rather than endpoint-derived channel names.

Compiler collision rules:

- public IDs are unique within their record namespace
- input IDs and state field IDs share the graph state namespace and may
  not collide
- output IDs are projection IDs and may intentionally mirror their source field
  IDs
- node IDs and edge IDs are separate public namespaces and are disambiguated by
  record kind in diagnostics and lowering metadata
- branch group names are unique within their source node
- user node IDs may not be `"$start"` or `"$finish"`
- runtime-generated channel IDs may not collide with any other generated runtime
  channel ID
- diagnostics for ID errors use the public graph path, record kind, and public ID
  whenever possible

## 7. Public Nodes And Runtime Graph Nodes

The word "node" appears in three places. They are related but not identical.

```text
Docket.Graph.Node
  Editable public node in the canonical graph.
  Used by UI, workflow compilers, diagnostics, host storage, and graph projection.
  Names implementation, node config, branch groups, and application metadata.

Docket.Node
  Behaviour implemented by executable node code.
  Declares config schema. Receives the current graph state, node config, and
  runtime context. Returns a partial state update, interrupt, await, or error.

Docket.Runtime.Graph.Node
  Internal runtime definition consumed by the Runtime.
  Names runtime subscriptions, outgoing edge references, executor settings, and
  normalized node config.
```

Example public node:

```elixir
%Docket.Graph.Node{
  id: "writer",
  label: "Write Draft",
  implementation: %{type: :module, module: Essay.Writer, function: :call},
  branches: %{},
  config: %{},
  policies: %{},
  metadata: %{}
}
```

Example runtime node definition:

```elixir
%Docket.Runtime.Graph.Node{
  id: "writer",
  module: Essay.Writer,
  function: :call,
  subscribe: ["edge:edge_start_writer"],
  outgoing_edges: ["edge_writer_finish"],
  metadata: %{
    public_node_id: "writer"
  }
}
```

The public node says "run this implementation with this config." The runtime
node says "activate this implementation from these subscriptions, pass the
current committed state snapshot, and evaluate these outgoing edges after
successful completion."

### 7.1 Node Type Contracts And State Updates

Executable node modules should be introspectable enough for graph editors and
the compiler to validate node-instance config. Data access is through the graph's
shared state snapshot, not through a separate node-local binding layer.

Public node behaviour shape (see `lib/docket/node.ex`):

```elixir
defmodule Docket.Node do
  @callback config_schema() :: Docket.Schema.t()

  @callback call(state :: map(), config :: map(), context :: map()) ::
              {:ok, state_update :: map()}
              | {:interrupt, Docket.Interrupt.t()}
              | {:await, term()}
              | {:error, term()}
end
```

`{:await, term()}` is reserved for post-v1 late-completion protocols and is
unsupported in v1: the dispatcher treats it as a permanent node failure.

`config_schema/0` returns the schema for node-instance configuration. The
compiler validates user-provided config and applies defaults. The normalized
config is passed to `call/3` along with the committed state snapshot and runtime
context.

For example, an LLM node can use prompt variables as state keys and output field
names as config:

```elixir
defmodule Docket.Nodes.LLM do
  @behaviour Docket.Node

  def config_schema do
    Docket.Schema.object(%{
      model: Docket.Schema.string(required: true),
      reasoning_effort: Docket.Schema.enum([:low, :medium, :high],
        default: :medium
      ),
      prompt_template: Docket.Schema.string(required: true),
      output_field: Docket.Schema.string(required: true)
    })
  end

  def call(state, config, context) do
    prompt = Docket.Template.render(config.prompt_template, state)
    {:ok, text} = context.llm_client.complete(config.model, prompt)

    {:ok, %{config.output_field => text}}
  end
end
```

A graph node instance stores the app user's config:

```elixir
%Docket.Graph.Node{
  id: "draft_reply",
  implementation: %{type: :module, module: Docket.Nodes.LLM, function: :call},
  config: %{
    model: "gpt-4.1-mini",
    reasoning_effort: :medium,
    prompt_template: "Reply to {{customer_message}} using {{account_context}}",
    output_field: "draft_response"
  }
}
```

The user-authored behaviour receives graph state:

```elixir
%{
  "customer_message" => "I need help with billing",
  "account_context" => "Enterprise account"
}
```

and returns a partial state update:

```elixir
{:ok, %{"draft_response" => "Thanks for reaching out..."}}
```

The runtime validates update keys against graph fields, validates values against
field schemas, applies reducers, then emits generated edge activations.

### 7.2 Schema Boundaries For State

`Docket.Schema` is the canonical typing language for graph data and node config.

Use `Docket.Schema` for:

- graph input fields
- graph state fields
- output projections through their source fields
- runtime channel definitions derived from those fields
- node config schemas

The compiler validates the boundary:

- node config conforms to `config_schema/0`
- prompt/state references discovered by editor tooling point at existing graph
  inputs or fields when the tool elects to enforce that
- node output field config points at existing graph fields when a node type uses
  configured output fields

Runtime validation should still validate state updates before reducers run:
unknown update fields, invalid values, oversized values, and reducer errors are
runtime errors. Compile-time validation protects graph shape; runtime validation
protects actual data.

`Docket.Schema` should be Docket's stable, serializable schema IR. It may compile
to a validation library such as Peri, and it may export JSON Schema for UI forms
or structured LLM outputs, but raw third-party schema terms should not be the
durable graph document format.

## 8. Compile, Verify, And Materialize

`Docket.Graph.Compiler` is the compiler module. It owns verification and runtime
materialization.

`compile/2` is the only compiler function that returns a compiled runtime
artifact. It materializes `Docket.Graph` into an internal
`Docket.Runtime.Graph` value.

`Docket.Graph.verify/2` is the public graph-centered verification API. It uses
the compiler rules and returns `{:ok, graph}` or `{:error, graph}`. In both
cases the returned graph is the graph document to keep using; on error, compiler
diagnostics are attached to `graph.diagnostics`.

`Docket.Graph.Compiler.verify/2` backs `Docket.Graph.verify/2` and has the same
return shape. Publish and start helpers may call verification internally, but
tests, host publishing flows, workflow compilers, and editor previews should use
`Docket.Graph.verify/2` when they need a stable way to ask whether a canonical
graph is runnable without starting a run or exposing runtime internals.

Applications do not normally store the runtime graph. They may verify that a
graph is runnable before publishing it:

```elixir
case Docket.Graph.verify(graph, opts) do
  {:ok, verified_graph} ->
    {:ok, MyApp.Graphs.save!(verified_graph, metadata)}

  {:error, verified_graph} ->
    {:error, verified_graph, Docket.Graph.diagnostics(verified_graph)}
end
```

The Runtime materializes at run start:

```elixir
graph = MyApp.Graphs.fetch!("essay-review", version: 5)

{:ok, run} =
  MyApp.Docket.run(graph, input, id: app_run_id)
```

The compiler lowers user-facing concepts:

```text
input field -> input channel
state field -> state channel
edge -> generated edge channel plus subscriptions
multi-source edge -> generated edge channel with barrier/all semantics
node branch group -> grouped guarded edge records plus generated edge channels
output -> output channel projection
Docket.Graph.Node -> Docket.Runtime.Graph.Node
```

The runtime graph remains derived from the canonical graph. If a host wants to
cache the runtime graph, that is a host implementation detail outside Docket's
required public contract.

## 9. Unified Graph Editing API

Build-time graph construction and realtime graph editing should use the same
interface.

Every non-bang public graph editing function should take a `Docket.Graph` and
return `{:ok, Docket.Graph.t()}` or `{:error, Docket.Graph.Error.t()}`. The bang
variant should return the updated graph or raise `Docket.Graph.Error`.

Editing functions do not compile or diagnose the graph. They clear stale
diagnostics so `graph.diagnostics` never pretends to describe a newer graph
shape. Callers explicitly use `Docket.Graph.verify/2` to attach fresh compiler
diagnostics, then read them with `Docket.Graph.diagnostics/2`.

That gives both calling styles without introducing a separate realtime patch or
operation API.

Build-time, pipe-oriented construction:

```elixir
graph =
  Docket.Graph.new!(id: "essay-review", name: "Essay Review")
  |> Docket.Graph.put_input!("topic", schema: Docket.Schema.string())
  |> Docket.Graph.put_field!("draft", schema: Docket.Schema.string())
  |> Docket.Graph.put_node!("writer", implementation: Essay.Writer)
  |> Docket.Graph.put_edge!("edge_start_writer", from: "$start", to: "writer")
  |> Docket.Graph.put_edge!("edge_writer_finish", from: "writer", to: "$finish")

case Docket.Graph.verify(graph) do
  {:ok, verified_graph} -> {:ok, verified_graph}
  {:error, verified_graph} -> {:error, verified_graph.diagnostics}
end
```

Realtime, event-by-event editing:

```elixir
case Docket.Graph.update_node(graph, "writer", %{label: "Draft Writer"}) do
  {:ok, graph} ->
    case Docket.Graph.verify(graph) do
      {:ok, verified_graph} -> {:ok, verified_graph}
      {:error, verified_graph} -> {:error, verified_graph.diagnostics}
    end

  {:error, reason} ->
    {:error, reason}
end
```

The difference is cadence. A compiler or hand-written graph may apply many bang
graph functions in a pipe. A UI may apply one non-bang graph function per user
action and run `Docket.Graph.verify/2` only when it wants compiler diagnostics
for the client.

### 9.1 Entry Point And Inputs

`Docket.Graph.new/1` is the normal tagged-result entry point for creating a
graph. `Docket.Graph.new!/1` is the pipe-friendly raising entry point. They are
the only graph editing functions that do not take an existing graph as their
first argument.

They create a canonical graph skeleton:

```elixir
{:ok, graph} = Docket.Graph.new(id: "essay-review", name: "Essay Review")
graph = Docket.Graph.new!(id: "essay-review", name: "Essay Review")
```

If `id:` is omitted, Docket generates a stable graph document ID and stores it
on `graph.id`. Host applications may use that ID as their primary graph ID or
store it alongside their own workflow, project, or version records.

That skeleton should be valid graph data, but it does not need to be runnable.
A fresh graph has no diagnostics until verification runs.

`Docket.Graph.put_input/4` does not add an executable input node. It adds an input
field to the graph:

```elixir
graph =
  graph
  |> Docket.Graph.put_input!("topic",
    schema: Docket.Schema.string(),
    required: true
  )
```

An input field is data supplied when a run starts. Runtime lowering turns it
into part of the initial state snapshot passed to nodes.

```elixir
graph =
  graph
  |> Docket.Graph.put_node!("writer", implementation: Essay.Writer)
```

In the canonical graph model, inputs are fields, not nodes. The executable entry
into the graph is still represented by start edges such as:

```elixir
Docket.Graph.put_edge!(graph, "edge_start_writer", from: "$start", to: "writer")
```

The full editing API (put/update/delete helpers for inputs, fields, outputs,
nodes, and edges, plus policy/metadata helpers, `diagnostics/2` and `verify/2`)
is documented in the `Docket.Graph`
module docs (`lib/docket/graph.ex`).

Compiler API:

```elixir
Docket.Graph.Compiler.verify(graph, opts \\ [])
Docket.Graph.Compiler.compile(graph, opts \\ [])
```

`verify/2` should return:

```elixir
{:ok, Docket.Graph.t()}
| {:error, Docket.Graph.t()}
```

`compile/2` should return:

```elixir
{:ok, Docket.Runtime.Graph.t()}
| {:error, Docket.Graph.t()}
```

The public API does not need a normal `decompile/2` path because host
applications store the canonical editable graph document directly.

## 10. Realtime And Build-Time Construction

Realtime construction means a graph may be temporarily incomplete or invalid
while a user is dragging nodes onto a canvas and connecting edges.

Build-time construction has the same property while a compiler is midway
through assembling a graph. A graph may be incomplete between function calls.

The editing API should be simple functional updates against `Docket.Graph`.
Callers pass an ID and the new shape, or an update function for that shape;
the `Docket.Graph` module docs cover the accepted attribute shapes.

Graph editor handlers can translate user actions into ordinary graph updates:

```text
add node -> put_node(graph, node_id, attrs)
move node -> update host-owned UI projection keyed by node_id
connect nodes -> put_edge(graph, edge_id, attrs)
edit edge condition -> update_edge(graph, edge_id, %{guard: guard})
delete edge -> delete_edge(graph, edge_id)
```

These functions update the canonical graph. They do not try to prove the graph
is executable, and they clear stale diagnostics from the returned graph. After
each update, callers can return the graph to the user or continue piping more
updates, then run `Docket.Graph.verify/2` when they want diagnostics.

Compiler diagnostics should allow incomplete work before verification:

- a node without all required reads can exist
- an edge can be missing a guard temporarily
- a graph can have no start path temporarily
- diagnostics are attached only when verification or compilation runs

The non-bang update helpers should return `{:ok, graph}` for ordinary
incomplete or invalid workflow states without diagnosing that graph. They should
only return tagged errors for programming errors such as a malformed argument
that cannot be represented as graph data at all. Bang helpers should raise
`Docket.Graph.Error` for those same hard failures. Runnable correctness is
checked by
`Docket.Graph.verify/2`, `Docket.Graph.Compiler.verify/2`, or
`Docket.Graph.Compiler.compile/2`.

## 11. Editing Existing Host Graph Versions

Published host graph artifacts are immutable. Editing an existing graph means
loading that canonical `Docket.Graph` document and using it as the starting point
for a new host-owned version.

Recommended flow:

```elixir
graph_v4 = MyApp.Graphs.fetch!("essay-review", version: 4)

draft =
  graph_v4
  |> Docket.Graph.put_node!("reviewer", %{
    implementation: %{type: :registered, id: "essay_reviewer"},
    label: "Reviewer"
  })
  |> Docket.Graph.put_edge!("edge_writer_reviewer", %{
    from: "writer",
    to: "reviewer"
  })
  |> Docket.Graph.metadata!(:based_on_artifact, "essay-review@4")

case Docket.Graph.verify(draft) do
  {:ok, verified_draft} ->
    {:ok, MyApp.Graphs.publish!(verified_draft, metadata)}

  {:error, verified_draft} ->
    {:error, verified_draft, Docket.Graph.diagnostics(verified_draft)}
end
```

Unpublished drafts may be modified in place in the host application's draft
store. Published versions should only be appended:

```text
published host graph artifact version 4
  -> copy/fetch canonical Docket.Graph
  -> edit draft in realtime
  -> preview compiler diagnostics
  -> verify runnable shape
  -> host saves/publishes immutable graph artifact version 5
```

If an application truly needs to change an existing published artifact, that
should be an administrative repair operation with explicit audit logging, not
the normal editing path.

## 12. Public Documents And Durable Persistence

`Docket.Graph` remains the editable public graph document. Docket intentionally
does not maintain a second generic map representation of that document.

Publishing compiles an effective diagnostic-free `%Docket.Graph{}`, stores it
through Docket's private versioned deterministic ETF codec, and returns a
`Docket.GraphRef`. Applications normally retain that reference on their own
workflow/version record. `Docket.Run` persistence is entirely backend-private;
applications keep the Run ID and business projections rather than copying a
second run document.

Rules:

- `Docket.Graph.new/1` accepts `id:`; Docket generates one when omitted.
- Publish through `save_graph` and treat the returned graph reference as
  immutable content identity.
- Start durable runs from `GraphRef`, and read them through
  `fetch_run`/`inspect_run` under an explicit tenant scope.
- Graph hashes cover the exact private ETF projection stored by the backend,
  excluding compiler diagnostics.
- The codec version and OTP-major deployment boundary are backend concerns.

## 13. UI Projection Boundary

The host application owns UI-specific projection. Docket should expose the
canonical `Docket.Graph` document, graph editing helpers, diagnostics,
checkpoints, and runtime events. The app can project those values into
its graph editor or inspector shape.

Editor events that change graph semantics should call ordinary graph update
helpers. Editor-only events should update host-owned projection state keyed by
public graph IDs:

```text
node position update -> update host-owned UI projection keyed by node_id
connect nodes -> put_edge(graph, edge_id, %{from: ..., to: ...})
delete edge -> delete_edge(graph, edge_id)
node property edit -> update_node(graph, node_id, attrs)
field panel edit -> put_field/update_field/delete_field
guard editor save -> update_edge(graph, edge_id, %{guard: guard})
```

The host app should persist the canonical `Docket.Graph` or its own
product-specific workflow record that can produce a `Docket.Graph`. It should
not persist UI canvas JSON as the canonical workflow document or store UI
projection state inside Docket graph documents.

## 14. Runtime Introspection And Realtime Overlays

Runtime introspection has two layers:

```text
static graph view:
  Docket.Graph -> host-owned UI projection

live run overlay:
  Run events/checkpoints/channel versions -> overlay data keyed by public IDs
```

The inspector loads the published graph through its `GraphRef`, whose private
hash is already recorded on the run, projects the graph to the app's UI shape,
then streams committed run events and applies overlay updates. Applications do
not recompute graph hashes from editable graph values.

Runtime events refer to runtime channels and runtime node IDs. The runtime
lowering map maps those IDs back to public node, edge, and field IDs for UI overlays.
That lowering map can be included in debug events or held inside the live
Runtime.

The UI can offer two modes:

```text
workflow mode:
  show Docket.Graph nodes, edges, fields, and user-facing statuses

runtime debug mode:
  reveal generated channels, barriers, subscriptions, and runtime node details
```

This avoids forcing normal users to understand generated edge channels while
still giving engineers a precise runtime inspection view.

## 15. Lowering Rules

### 15.1 Inputs

Public graph:

```elixir
input "topic", schema: string()
```

Runtime:

```text
Runtime.Graph.Channel "input:topic"
  type: LastValue
```

### 15.2 State Fields

Public graph:

```elixir
field "draft", schema: string(), reducer: last_value()
```

Runtime:

```text
Runtime.Graph.Channel "state:draft"
  type: LastValue
```

### 15.3 Simple Edges

Public graph:

```elixir
edge "writer", "reviewer", id: "edge_writer_reviewer"
```

Runtime:

```text
Runtime.Graph.Channel "edge:edge_writer_reviewer"
  type: Ephemeral

Runtime.Graph.Node "writer"
  outgoing_edges: ["edge_writer_reviewer"]

Runtime.Graph.Node "reviewer"
  subscribe: ["edge:edge_writer_reviewer"]
```

User node code does not manually write edge signal channels. The Runtime emits
compiler-generated edge activations after successful source-node completion.
An unguarded edge triggers whenever its source node completes successfully and
the superstep commits. A guarded edge first becomes a candidate from successful
source-node completion, then its guard is evaluated against the newly committed
state and the changed fields from that same update barrier. Only guard-true
edges activate their target node in the next superstep.

### 15.4 Fan-Out

Public graph:

```elixir
edge "researcher", "summarizer", id: "edge_researcher_summarizer"
edge "researcher", "tester", id: "edge_researcher_tester"
```

Runtime:

```text
edge:edge_researcher_summarizer
edge:edge_researcher_tester
```

Fan-out helpers may accept multiple targets, but the canonical graph still stores
one edge record per target so every runtime activation channel has a stable
`edge:<edge_id>` identity.

### 15.5 Fan-In

Public graph:

```elixir
edge ["researcher", "tester"], "reviewer", id: "edge_review_ready"
```

Runtime:

```text
edge:edge_review_ready
  type: barrier/all
```

The canonical graph preserves fan-in intent as one edge record whose `from`
value is a list of source node IDs. Runtime lowering may implement that edge
with internal barrier bookkeeping, but the public runtime activation identity is
still `edge:<edge_id>`. Editors, diagnostics, lowering metadata, and workflow
compilers can keep the user's intent without a separate public join record.
For multi-source edges, successful completion of each source contributes to the
barrier. The edge triggers in the next superstep only after all sources are
satisfied for that barrier window and, if present, the edge guard is true.

### 15.6 Conditional Edges

Public graph:

```elixir
edge "reviewer", "deploy",
  id: "edge_review_approved",
  guard: equals(path("review", ["status"]), "approved")
```

Runtime:

```text
edge:edge_review_approved
```

The guarded edge is a completion edge filtered by committed state. The source
node completing successfully creates an activation candidate; after the update
barrier commits state updates, the Runtime evaluates the edge guard against the
new committed state and the changed field set. If the guard is true, the edge
activates its target in the next superstep. If the guard is false, no activation
is emitted for that edge.

### 15.7 Branches

Public graph:

```elixir
graph =
  graph
  |> Docket.Graph.put_node!("reviewer",
    branches: %{
      "decision" => ["edge_review_approved", "edge_review_rejected"]
    }
  )
  |> Docket.Graph.put_edge!("edge_review_approved",
    from: "reviewer",
    to: "deploy",
    guard: equals(path("review", ["status"]), "approved")
  )
  |> Docket.Graph.put_edge!("edge_review_rejected",
    from: "reviewer",
    to: "revise",
    guard: equals(path("review", ["status"]), "rejected")
  )
```

Runtime:

```text
edge:edge_review_approved
edge:edge_review_rejected
```

The canonical graph preserves branch groups as node-local editing and inspection
metadata over outgoing guarded edge records. Lowering keeps the guarded edge
records and generated `edge:<edge_id>` activation channels as the execution
surface. The Runtime does not need to know whether an edge appears in a branch
group; the lowering map can keep the source node ID, branch group name, and edge
IDs available for diagnostics and runtime overlays. v1 does not generate
separate branch activation channels.

## 16. Diagnostics And Runtime Verification

`Docket.Graph` diagnostics are compiler output attached to the graph document.
They are not a separate editing subsystem and graph edit helpers do not produce
them.

Compiler diagnostics:

- produce diagnostics for UI display
- allow incomplete graphs
- support realtime editing through explicit verification
- may warn about missing fields, disconnected nodes, missing start edges, or
  incomplete node configuration
- should use public IDs and UI paths whenever possible
- should not prevent saving or continuing to edit the graph

Runtime verification is the blocking gate. `Docket.Graph.verify/2` and
`Docket.Graph.Compiler.verify/2` return `{:ok, graph}` or `{:error, graph}`.
On error, diagnostics are attached to the returned graph.
`Docket.Graph.Compiler.compile/2` returns a compiled runtime graph or
`{:error, graph_with_diagnostics}`. Compile and run paths must reject graphs
that cannot run safely.

Runtime verification rejects:

- references that cannot resolve
- edges with invalid endpoints
- duplicate IDs that cannot be represented safely
- invalid start or activation paths
- schema, reducer, or guard errors
- impossible multi-source edge barriers
- cycles without limits or halt conditions
- node implementation references rejected by policy

Runtime lowering validation:

- rejects generated channel ID collisions
- rejects subscriptions to missing channels
- rejects writes to undeclared channels
- verifies every runtime ID maps back to public intent for debugging

Diagnostics should use public IDs whenever possible, even when verification
returns them as blocking errors:

```elixir
%Docket.Graph.Diagnostic{
  severity: :error,
  code: :unknown_field,
  message: "node writer config references unknown field review",
  path: [:nodes, "writer", :config, :prompt_template],
  public_id: "writer",
  runtime_id: nil
}
```

## 17. Persistence Responsibilities

Docket graph and run persistence is host-owned. The host application may persist
editable drafts as:

- canonical `Docket.Graph` values
- product-specific workflow records that can produce `Docket.Graph` values

The host also persists immutable published graph artifacts containing `Docket.Graph` and
`Docket.Run` documents emitted by checkpoints.

Recommended split:

```text
Host graph records:
  editable Docket.Graph
  immutable published graph artifacts containing Docket.Graph
  collaboration metadata
  host-owned UI layout keyed by Docket graph IDs
  product ownership and permissions

Host run records:
  latest Docket.Run document
  status, ownership, and indexing fields
  project, user, session, message, or job relationships
```

Host storage does not need to know about `Docket.Runtime.Graph` or interpret
`Docket.Run` execution internals. Runtime materialization happens when Docket
verifies a graph, starts a run, resumes a run, or retries a run.

## 18. WaterCooler Workflow Compiler

WaterCooler should compile workflow records into `Docket.Graph`, not directly
into runtime channels.

Mapping:

```text
workflow definition -> Docket.Graph
workflow step -> Docket.Graph.Node
gate -> guarded edge, node-local branch group, or multi-source edge
step result -> graph field write
workflow canvas layout -> host-owned UI projection keyed by graph IDs
published workflow version -> immutable host graph artifact containing Docket.Graph
current_step_id -> compatibility projection over run overlay/frontier
RuntimeChannel execution -> Executor adapter
```

Compatibility compiler shape:

```elixir
defmodule WaterCooler.Docket.WorkflowCompiler do
  def to_graph(workflow, opts) do
    Docket.Graph.new!(id: workflow.id, name: workflow.name)
    |> add_inputs(workflow)
    |> add_fields(workflow)
    |> add_nodes(workflow)
    |> add_edges_and_gates(workflow)
    |> add_outputs(workflow)
  end

  def verify(workflow, opts) do
    workflow
    |> to_graph(opts)
    |> Docket.Graph.verify(opts)
  end
end
```

## 19. End-To-End Editing Flow

```text
1. User opens workflow editor.
2. Host loads latest published Docket.Graph or existing draft Docket.Graph.
3. Host projects Docket.Graph to its graph editor UI shape.
4. User drags a node onto the canvas.
5. UI sends the node ID and node shape.
6. Server calls `put_node/4` on the graph.
7. Server returns updated projection.
8. User connects nodes.
9. UI sends the edge ID and edge shape.
10. Server calls `put_edge/4`.
11. User clicks publish.
12. Docket.Graph.verify/2 runs blocking runtime verification and attaches diagnostics.
13. Host saves immutable graph artifact version N+1 if diagnostics are empty.
14. New runs compute and capture the new graph hash; active runs stay on their
    original graph hash.
```

## 20. End-To-End Runtime Inspection Flow

```text
1. User opens run inspector.
2. Server loads the latest Docket.Run document.
3. Server fetches a canonical Docket.Graph document using host-owned graph
   identity, then verifies its computed hash against run.graph_hash.
4. Server projects Docket.Graph to its run inspector UI shape.
5. Server streams committed run events.
6. Server maps runtime channel/node IDs to public IDs using lowering metadata.
7. UI overlays active/completed/failed/waiting status on public nodes and edges.
8. User can switch to runtime debug mode to inspect generated channels/barriers.
```

## 21. LangGraph Reference Notes

LangGraph is a useful reference, but Docket should not copy it exactly.

In LangGraph, users define a `StateGraph` with state schema, nodes, and edges.
Compilation validates the graph and lowers it to a Pregel-style runtime graph
with channels, triggers, and writers. The direct Pregel API can also be used by
hand, but ordinary users generally work through `StateGraph`.

LangGraph's builder keeps these concepts separate before compile:

```text
edges -> normal fixed transitions
waiting_edges -> fan-in joins that wait for multiple starts
branches -> conditional routing specs keyed by source node and branch name
```

Compilation then attaches normal edges, waiting edges, and branches to the
runtime graph. At runtime, the compiled Pregel graph exposes channels such as
state channels, start/activation channels, branch-specific channels, and
barrier-like channels for waits. Docket should follow that separation of public
intent from runtime lowering without copying LangGraph's exact API; in Docket v1,
branch activation is represented by guarded `edge:<edge_id>` channels.

The useful lowering pattern is:

```text
state schema fields -> runtime channels
node -> Pregel node that reads subscribed state channels and writes updates
simple edge A -> B -> A writes to B's generated activation channel
fan-in edge [A, B] -> C -> edge activation channel with barrier/all semantics
conditional edge -> guarded edge activation channel(s)
```

Docket should borrow that lowering model, but expose a cleaner storage and
editing model:

```text
Docket.Graph is canonical and editable.
Public topology is stored as edge records.
Fan-in joins are multi-source edges.
Branch groups are node-local metadata over outgoing guarded edges.
Docket.Run is canonical and restorable.
Docket.Runtime.Graph is internal and derived.
Host applications store Docket documents, not runtime graph internals.
```

LangGraph does not appear to expose first-class `update_node`, `delete_node`,
`update_edge`, or `delete_edge` helpers on an already compiled graph. Its
builder methods warn when called after compile because those edits are not
reflected in the compiled graph. Docket's editing story should stay centered on
simple updates to canonical `Docket.Graph` values:

```elixir
{:ok, graph} = Docket.Graph.update_node(graph, node_id, attrs)
{:ok, graph} = Docket.Graph.update_edge(graph, edge_id, attrs)
```

LangGraph's migration guidance also supports keeping active runs pinned to the
host graph artifact they started with. Docket should keep the simple v1 rule,
expressed in terms of Docket's content fingerprint:

```text
new edits append a new host graph artifact;
active runs stay on the graph hash they started with.
```

Sources:

- https://docs.langchain.com/oss/python/langgraph/graph-api
- https://docs.langchain.com/oss/python/langgraph/pregel
- https://github.com/langchain-ai/langgraph/blob/main/libs/langgraph/langgraph/graph/state.py

## 22. Resolved v1 Graph Construction Scope

Resolved v1 graph construction scope:

1. `Docket.Graph` as the canonical public graph document.
2. One functional graph editing API for both build-time pipes and realtime UI
   edits.
3. Graph helpers for fields, inputs, nodes, edges, outputs, policies, and
   metadata.
4. App-owned graph persistence with optional `id:` generation.
5. Compiler diagnostics attached to `Docket.Graph` and blocking compiler
   verification.
6. `Docket.Graph.verify/2` plus `Docket.Graph.Compiler.verify/2` and
   `compile/2`.
7. Internal `Docket.Runtime.Graph` materialization returned by `compile/2`.
8. Runtime overlay mapping from events/channels back to public IDs.
9. WaterCooler/sequential workflow compatibility compiler remains post-v1. It
   should compile through `Docket.Graph` when added, but it is not part of the v1
   implementation gate.

## 23. Resolved Decisions

1. `Docket.Graph.verify/2` is the public graph-centered verification API. It is
   useful for tests, editor previews, workflow compiler tests, and host-owned
   publish flows. Start/publish helpers may still call compiler verification
   internally.
2. Branch and join sugar are represented through the existing public graph
   records: fan-in joins are multi-source edges, and branch groups are
   node-local metadata over outgoing guarded edge IDs. The compiler normalizes
   those records into runtime activation channels, guards, and barrier
   semantics, so the Runtime only consumes `Docket.Runtime.Graph`.
3. Collaborative editing revisions are out of scope for v1 and remain
   host-owned.
4. The internal executable graph is named `Docket.Runtime.Graph`, with
   `Docket.Runtime.Graph.Node`, `Docket.Runtime.Graph.Channel`, and
   `Docket.Runtime.Graph.Lowering` underneath it.

## 24. Strong Recommendations

1. Treat `Docket.Graph` as the canonical public graph.
2. Treat `Docket.Runtime.Graph` as internal derived runtime materialization.
3. Keep graph persistence app-owned; Docket should accept graph documents as
   values.
4. Treat `Docket.Run` as the canonical public resume document emitted by
   checkpoints.
5. Keep published host graph artifacts immutable and append-only.
6. Support editing existing host graph artifacts by fetching the canonical graph
   and appending a new host-owned version after edits.
7. Make build-time and realtime graph construction use the same graph update
   helpers.
8. Keep UI projection host-owned and outside Docket graph documents;
   `Docket.Graph` is the canonical workflow document.
9. Show generated channels only in runtime debug mode.
10. Build graph verify and compiler compile early because they will reveal API
    mistakes quickly.
