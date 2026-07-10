defmodule Docket.Runtime.Graph.ArtifactTest do
  use Docket.Test.Case, async: true

  alias Docket.Runtime.Graph.Artifact

  test "round-trips compiled runtime graphs through a strict JSON-safe artifact" do
    for graph <- [Graphs.minimal_linear(), Graphs.guarded_edge(), Graphs.interrupt_review()] do
      canonical = graph |> Docket.Graph.to_map() |> Docket.Graph.from_map!()
      runtime = compile!(canonical)
      artifact = Artifact.dump(runtime)

      assert {:ok, ^runtime} = Artifact.load(artifact, runtime.graph_id, runtime.graph_hash)
      assert is_binary(Docket.Graph.Serializer.canonical_json_encode(artifact))
      assert artifact["compiler_abi"] == Artifact.compiler_abi()
    end
  end

  test "hydrates a node module in a fresh BEAM before resolving its callback atom" do
    runtime = compile!(Graphs.minimal_linear())
    artifact = runtime |> Artifact.dump() |> :erlang.term_to_binary() |> Base.encode64()

    code = """
    artifact =
      "DOCKET_GRAPH_ARTIFACT"
      |> System.fetch_env!()
      |> Base.decode64!()
      |> :erlang.binary_to_term([:safe])

    case Docket.Runtime.Graph.Artifact.load(
           artifact,
           #{inspect(runtime.graph_id)},
           #{inspect(runtime.graph_hash)}
         ) do
      {:ok, _runtime} -> IO.write("hydrated")
      other -> raise "cold artifact hydration failed: \#{inspect(other)}"
    end
    """

    code_paths =
      Mix.Project.build_path()
      |> Path.join("lib/*/ebin")
      |> Path.wildcard()
      |> Enum.flat_map(&["-pa", &1])

    elixir = System.find_executable("elixir") || flunk("elixir executable was not found")

    assert {"hydrated", 0} =
             System.cmd(elixir, code_paths ++ ["-e", code],
               env: [{"DOCKET_GRAPH_ARTIFACT", artifact}],
               stderr_to_stdout: true
             )
  end

  test "rejects tampering and mismatched content addresses" do
    runtime = compile!(Graphs.minimal_linear())
    artifact = Artifact.dump(runtime)

    assert {:error, %Docket.Error{type: :invalid_graph_artifact}} =
             Artifact.load(
               put_in(artifact["runtime"]["id"], "tampered"),
               runtime.graph_id,
               runtime.graph_hash
             )

    assert {:error, %Docket.Error{type: :invalid_graph_artifact}} =
             Artifact.load(artifact, runtime.graph_id, "wrong")

    unknown = Map.put(artifact, "future_key", true)

    assert {:error, %Docket.Error{type: :invalid_graph_artifact}} =
             Artifact.load(unknown, runtime.graph_id, runtime.graph_hash)
  end
end
