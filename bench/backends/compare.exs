# Compares two bench/backends result files, e.g. main against a branch:
#
#     mix run bench/backends/compare.exs -- tmp/bench/backends/main-*.json tmp/bench/backends/my-branch-*.json
#
# Prints each backend/scenario pair present in both files with the change in
# throughput and p50/p99 latency. Higher runs/s and lower latency are better.

[base_path, head_path] =
  case Enum.drop_while(System.argv(), &(&1 == "--")) do
    [a, b] -> [a, b]
    _ -> raise ArgumentError, "usage: compare.exs BASE.json HEAD.json"
  end

load = fn path ->
  data = path |> File.read!() |> JSON.decode!()
  {data, Map.new(data["results"], &{{&1["backend"], &1["scenario"]}, &1})}
end

{base_meta, base} = load.(base_path)
{head_meta, head} = load.(head_path)

IO.puts(
  "base: #{base_meta["label"]} @ #{base_meta["git"]}   head: #{head_meta["label"]} @ #{head_meta["git"]}\n"
)

pct = fn
  nil, _ ->
    "      -"

  _, nil ->
    "      -"

  a, b when a == 0 ->
    if b == 0, do: "     0%", else: "      -"

  a, b ->
    (100 * (b - a) / a)
    |> round()
    |> then(&String.pad_leading("#{if &1 > 0, do: "+", else: ""}#{&1}%", 7))
end

fmt = fn
  nil -> "-"
  v when is_float(v) -> :erlang.float_to_binary(v, decimals: 1)
  v -> to_string(v)
end

IO.puts(
  String.pad_trailing("backend/scenario", 28) <>
    String.pad_leading("runs/s base→head", 26) <>
    String.pad_leading("p50 ms", 22) <> String.pad_leading("p99 ms", 22)
)

for key <- base |> Map.keys() |> Enum.filter(&Map.has_key?(head, &1)) |> Enum.sort() do
  {b, h} = {base[key], head[key]}
  {backend, scenario} = key
  lat = fn r, p -> get_in(r, ["latency_ms", p]) end

  cell = fn bv, hv, width ->
    String.pad_leading("#{fmt.(bv)}→#{fmt.(hv)} #{pct.(bv, hv)}", width)
  end

  IO.puts(
    String.pad_trailing("#{backend}/#{scenario}", 28) <>
      cell.(b["runs_per_sec"], h["runs_per_sec"], 26) <>
      cell.(lat.(b, "p50"), lat.(h, "p50"), 22) <>
      cell.(lat.(b, "p99"), lat.(h, "p99"), 22)
  )
end
