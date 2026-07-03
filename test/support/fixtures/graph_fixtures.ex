defmodule Docket.Test.Fixtures.Graphs do
  @moduledoc """
  Small canonical graph fixtures named for the behavior they prove.

  Fixtures follow the catalogs in `docket-v1-test-suite-design.md` section 6
  and `docket-compiler-design.md` section 15.5. They are plain values; no
  fixture requires processes or external services.
  """

  alias Docket.Test.Fixtures.Nodes
  alias Docket.{Graph, Guard, Reducer, Schema}

  @doc """
  input: value -> copy -> output: result

  The smallest runnable graph with an output projection.
  """
  def minimal_linear do
    Graph.new!(id: "minimal-linear")
    |> Graph.put_input!("value", schema: Schema.string(), required: true)
    |> Graph.put_field!("result", schema: Schema.string(), reducer: Reducer.last_value())
    |> Graph.put_node!("copy",
      implementation: Nodes.CopyInput,
      config: %{from: "value", to: "result"}
    )
    |> Graph.put_edge!("edge_start_copy", from: "$start", to: "copy")
    |> Graph.put_edge!("edge_copy_finish", from: "copy", to: "$finish")
    |> Graph.put_output!("result", [])
  end

  @doc """
  start -> writer -> reviewer -> finish

  Proves generated edge channel lowering and sequential activation.
  """
  def simple_edge do
    Graph.new!(id: "simple-edge")
    |> Graph.put_input!("topic", schema: Schema.string(), required: true)
    |> Graph.put_field!("draft", schema: Schema.string(), reducer: Reducer.last_value())
    |> Graph.put_field!("review", schema: Schema.map())
    |> Graph.put_node!("writer", implementation: Nodes.Echo)
    |> Graph.put_node!("reviewer", implementation: Nodes.Echo)
    |> Graph.put_edge!("edge_start_writer", from: "$start", to: "writer")
    |> Graph.put_edge!("edge_writer_reviewer", from: "writer", to: "reviewer")
    |> Graph.put_edge!("edge_reviewer_finish", from: "reviewer", to: "$finish")
    |> Graph.put_output!("draft", [])
  end

  @doc """
  start -> source; source -> left; source -> right

  Proves fan-out lowering: one activation channel per edge.
  """
  def fanout do
    Graph.new!(id: "fanout")
    |> Graph.put_input!("value", schema: Schema.string())
    |> Graph.put_field!("left_out", schema: Schema.string())
    |> Graph.put_field!("right_out", schema: Schema.string())
    |> Graph.put_node!("source", implementation: Nodes.Echo)
    |> Graph.put_node!("left", implementation: Nodes.Echo)
    |> Graph.put_node!("right", implementation: Nodes.Echo)
    |> Graph.put_edge!("edge_start_source", from: "$start", to: "source")
    |> Graph.put_edge!("edge_source_left", from: "source", to: "left")
    |> Graph.put_edge!("edge_source_right", from: "source", to: "right")
  end

  @doc """
  fanout plus edge [left, right] -> combine

  Proves barrier/all lowering for multi-source edges.
  """
  def multi_source_edge do
    fanout()
    |> with_id("multi-source-edge")
    |> Graph.put_node!("combine", implementation: Nodes.Echo)
    |> Graph.put_edge!("edge_combine_ready", from: ["left", "right"], to: "combine")
    |> Graph.put_edge!("edge_combine_finish", from: "combine", to: "$finish")
  end

  @doc """
  start -> fetch; fetch -> premium_step / standard_step guarded on user input

  Proves guard expression compilation against input channels.
  """
  def guarded_edge do
    premium = Guard.equals(Guard.path("user", ["premium_user"]), true)

    Graph.new!(id: "guarded-edge")
    |> Graph.put_input!("user", schema: Schema.map(), required: true)
    |> Graph.put_field!("plan", schema: Schema.string())
    |> Graph.put_node!("fetch", implementation: Nodes.Echo)
    |> Graph.put_node!("premium_step", implementation: Nodes.Echo)
    |> Graph.put_node!("standard_step", implementation: Nodes.Echo)
    |> Graph.put_edge!("edge_start_fetch", from: "$start", to: "fetch")
    |> Graph.put_edge!("edge_premium", from: "fetch", to: "premium_step", guard: premium)
    |> Graph.put_edge!("edge_standard",
      from: "fetch",
      to: "standard_step",
      guard: Guard.not(premium)
    )
  end

  @doc """
  reviewer groups its guarded outgoing edges under the "decision" branch.

  Proves branch groups lower to metadata over guarded edges only.
  """
  def branch_group do
    approved = Guard.equals(Guard.path("review", ["status"]), "approved")

    Graph.new!(id: "branch-group")
    |> Graph.put_field!("review", schema: Schema.map())
    |> Graph.put_node!("reviewer",
      implementation: Nodes.Echo,
      branches: %{"decision" => ["edge_approved", "edge_rejected"]}
    )
    |> Graph.put_node!("publish", implementation: Nodes.Echo)
    |> Graph.put_node!("revise", implementation: Nodes.Echo)
    |> Graph.put_edge!("edge_start_reviewer", from: "$start", to: "reviewer")
    |> Graph.put_edge!("edge_approved", from: "reviewer", to: "publish", guard: approved)
    |> Graph.put_edge!("edge_rejected",
      from: "reviewer",
      to: "revise",
      guard: Guard.not(approved)
    )
  end

  @doc """
  start -> increment -> decide; decide loops back while under the limit.

  Proves cycles compile when a max-supersteps policy bounds them.
  """
  def cycle_counter do
    Graph.new!(id: "cycle-counter")
    |> Graph.put_field!("count", schema: Schema.float(), default: 0.0)
    |> Graph.put_node!("increment", implementation: Nodes.Echo)
    |> Graph.put_node!("decide", implementation: Nodes.Echo)
    |> Graph.put_edge!("edge_start_increment", from: "$start", to: "increment")
    |> Graph.put_edge!("edge_increment_decide", from: "increment", to: "decide")
    |> Graph.put_edge!("edge_loop",
      from: "decide",
      to: "increment",
      guard: Guard.not(Guard.equals(Guard.path("count", []), 10.0))
    )
    |> Graph.put_edge!("edge_done",
      from: "decide",
      to: "$finish",
      guard: Guard.equals(Guard.path("count", []), 10.0)
    )
    |> Graph.policy!("max_supersteps", 50)
  end

  @doc """
  Node config declares a key its config schema does not accept.
  """
  def unknown_config_field do
    minimal_linear()
    |> with_id("unknown-config-field")
    |> Graph.update_node!("copy", config: %{from: "value", to: "result", bogus: "x"})
  end

  @doc """
  Edge targets a node that does not exist.
  """
  def invalid_unknown_target do
    minimal_linear()
    |> with_id("invalid-unknown-target")
    |> Graph.put_edge!("edge_copy_ghost", from: "copy", to: "ghost")
  end

  @doc """
  Guard references a field that does not exist.
  """
  def invalid_guard do
    minimal_linear()
    |> with_id("invalid-guard")
    |> Graph.update_edge!("edge_copy_finish", guard: Docket.Guard.changed("missing_field"))
  end

  defp with_id(graph, id), do: %{graph | id: id}
end
