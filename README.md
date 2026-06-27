# Docket

Docket is an Elixir library for durable, graph-based workflow execution.

This repository is currently scaffolded as a minimal Mix project. The runtime,
graph construction API, and test suite designs live under `docs/architecture/`.

Start with `docs/architecture/docket-v1-implementation-path.md`. Docket v1 is
organized around two flows: building a canonical `Docket.Graph` document and
running that graph document into checkpointed `Docket.Run` snapshots.

## Examples

- `examples/parent-app-integration.md` shows how a parent app configures Docket,
  starts runs with app-owned metadata, persists checkpoints, and resumes durable
  runs.
- `examples/llm-node.md` shows a generic LLM node implementation with
  config schema, dynamic input/output ports, and graph channel bindings.
