defmodule Docket.Bench.Compare.NoopNode do
  @behaviour Docket.Node

  @impl true
  def config_schema, do: Docket.Schema.object(%{})

  @impl true
  def call(_state, _config, _context), do: {:ok, %{}}
end

defmodule Docket.Bench.Compare.Runner do
  alias Docket.Bench.Compare.NoopNode

  @scenarios [
    {"single_node", {:chain, 1}},
    {"chain_10", {:chain, 10}},
    {"fanout_8", {:fanout, 8}}
  ]

  def main(argv) do
    {opts, positional} =
      OptionParser.parse!(argv,
        strict: [target_seconds: :float, repeats: :integer, warmup: :integer]
      )

    if positional != [] do
      raise ArgumentError, "unexpected positional arguments: #{inspect(positional)}"
    end

    target_seconds = Keyword.get(opts, :target_seconds, 0.5)
    repeats = Keyword.get(opts, :repeats, 7)
    warmup = Keyword.get(opts, :warmup, 50)

    unless target_seconds > 0 and repeats >= 3 and warmup >= 0 do
      raise ArgumentError,
            "target_seconds must be positive, repeats at least 3, and warmup non-negative"
    end

    IO.puts(
      "META,docket,#{System.version()},#{System.otp_release()},#{target_seconds},#{repeats},#{warmup}"
    )

    Enum.each(@scenarios, fn {name, shape} ->
      runtime_graph = shape |> graph() |> compile!()
      invoke = fn -> invoke!(runtime_graph) end

      repeat(invoke, warmup)
      iterations = calibrate(invoke, target_seconds)

      samples =
        for _ <- 1..repeats do
          :erlang.garbage_collect()
          elapsed_ns = time_ns(fn -> repeat(invoke, iterations) end)
          elapsed_ns / iterations
        end

      emit(name, iterations, samples)
    end)
  end

  defp graph({:chain, count}) do
    graph =
      Docket.Graph.new!(id: "compare-chain-#{count}")
      |> Docket.Graph.put_input!("token", schema: :integer, required: true)

    node_ids = for index <- 1..count, do: "node_#{index}"

    graph =
      Enum.reduce(node_ids, graph, fn node_id, acc ->
        Docket.Graph.put_node!(acc, node_id, implementation: NoopNode)
      end)

    graph =
      graph
      |> Docket.Graph.put_edge!("start-node-1", from: "$start", to: hd(node_ids))

    graph =
      node_ids
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.with_index(1)
      |> Enum.reduce(graph, fn {[from, to], index}, acc ->
        Docket.Graph.put_edge!(acc, "chain-#{index}", from: from, to: to)
      end)

    Docket.Graph.put_edge!(graph, "last-finish", from: List.last(node_ids), to: "$finish")
  end

  defp graph({:fanout, count}) do
    graph =
      Docket.Graph.new!(id: "compare-fanout-#{count}")
      |> Docket.Graph.put_input!("token", schema: :integer, required: true)

    Enum.reduce(1..count, graph, fn index, acc ->
      node_id = "node_#{index}"

      acc
      |> Docket.Graph.put_node!(node_id, implementation: NoopNode)
      |> Docket.Graph.put_edge!("start-#{index}", from: "$start", to: node_id)
      |> Docket.Graph.put_edge!("finish-#{index}", from: node_id, to: "$finish")
    end)
  end

  defp compile!(graph) do
    case Docket.Graph.Compiler.compile(graph, profile: :run) do
      {:ok, runtime_graph} -> runtime_graph
      {:error, failed_graph} -> raise "benchmark graph did not compile: #{inspect(failed_graph)}"
    end
  end

  defp invoke!(runtime_graph) do
    case Docket.Test.run_inline(runtime_graph, %{"token" => 0}, run_id: "compare-run") do
      {:ok, %{status: :done}, _checkpoints} -> :ok
      other -> raise "benchmark invocation failed: #{inspect(other)}"
    end
  end

  defp calibrate(invoke, target_seconds) do
    probe_iterations = 10
    probe_ns = max(time_ns(fn -> repeat(invoke, probe_iterations) end), 1)
    target_ns = target_seconds * 1_000_000_000

    target_ns
    |> Kernel.*(probe_iterations)
    |> Kernel./(probe_ns)
    |> round()
    |> max(10)
    |> min(5_000)
  end

  defp repeat(_invoke, 0), do: :ok

  defp repeat(invoke, count) do
    Enum.each(1..count, fn _ -> invoke.() end)
  end

  defp time_ns(fun) do
    started = System.monotonic_time()
    fun.()
    System.convert_time_unit(System.monotonic_time() - started, :native, :nanosecond)
  end

  defp emit(name, iterations, samples) do
    sorted = Enum.sort(samples)
    median = percentile(sorted, 0.50)
    p95 = percentile(sorted, 0.95)
    encoded_samples = Enum.map_join(samples, "|", &format_number/1)

    IO.puts(
      Enum.join(
        [
          "RESULT",
          "docket",
          name,
          "inline",
          iterations,
          format_number(median),
          format_number(p95),
          format_number(hd(sorted)),
          format_number(List.last(sorted)),
          encoded_samples
        ],
        ","
      )
    )
  end

  defp percentile(sorted, fraction) do
    index = max(ceil(length(sorted) * fraction) - 1, 0)
    Enum.at(sorted, index)
  end

  defp format_number(value) when is_integer(value), do: Integer.to_string(value)
  defp format_number(value), do: :erlang.float_to_binary(value, decimals: 3)
end

Docket.Bench.Compare.Runner.main(System.argv())
