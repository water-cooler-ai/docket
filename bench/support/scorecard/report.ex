defmodule Docket.Bench.Scorecard.Report do
  @moduledoc "Console scorecard table plus manifest.json and scorecard.json artifact writers."

  def render(results, meta) do
    output = results |> build_lines(meta) |> Enum.join("\n")
    IO.puts(output)
    output
  end

  def write_manifest!(dir, manifest) do
    path = Path.join(dir, "manifest.json")
    write_json!(path, manifest)
    path
  end

  def write_scorecard!(dir, scorecard) do
    path = Path.join(dir, "scorecard.json")
    write_json!(path, scorecard)
    path
  end

  defp build_lines(results, meta) do
    header = {"Metric", "Scenario", "Score", "Evidence"}
    rows = Enum.map(results, fn r -> {r.metric, label(r), score_str(r.score), r.evidence} end)
    all = [header | rows]

    w1 = width(all, fn {c, _, _, _} -> c end)
    w2 = width(all, fn {_, c, _, _} -> c end)
    w3 = width(all, fn {_, _, c, _} -> c end)
    w4 = width(all, fn {_, _, _, c} -> c end)

    separator = String.duplicate("─", w1 + 2 + w2 + 2 + w3 + 2 + w4)

    [
      title(meta),
      separator,
      row(header, {w1, w2, w3})
    ] ++
      Enum.map(rows, &row(&1, {w1, w2, w3})) ++
      [
        separator,
        invariant_line(results, {w1, w2, w3}),
        overall_line(results, {w1, w2, w3})
      ]
  end

  defp title(meta) do
    dirty = if meta.git.dirty, do: "*", else: ""
    sha7 = String.slice(meta.git.sha, 0, 7)

    "Docket Scorecard  ·  #{sha7}#{dirty}  ·  profile #{meta.profile}  ·  PG #{meta.pg}  ·  seed #{meta.seed}"
  end

  defp row({c1, c2, c3, c4}, {w1, w2, w3}) do
    String.pad_trailing(c1, w1) <>
      "  " <>
      String.pad_trailing(c2, w2) <>
      "  " <> String.pad_leading(c3, w3) <> "  " <> c4
  end

  defp invariant_line(results, {w1, w2, w3}) do
    total = Enum.sum(Enum.map(results, &length(&1.invariants)))
    scenarios = Enum.count(results, &(&1.invariants != []))
    all_pass = Enum.all?(results, fn r -> Enum.all?(r.invariants, & &1.pass) end)
    status = if all_pass, do: "PASS", else: "FAIL"

    String.pad_trailing("Invariants", w1) <>
      "  " <>
      String.pad_trailing("#{total} checks / #{scenarios} scenarios", w2) <>
      "  " <> String.pad_leading(status, w3)
  end

  defp overall_line(results, {w1, w2, w3}) do
    String.pad_trailing("Overall", w1) <>
      "  " <> String.pad_trailing("", w2) <> "  " <> String.pad_leading(overall(results), w3)
  end

  defp overall(results) do
    gated = Enum.find(results, fn r -> Enum.any?(r.invariants, &(not &1.pass)) end)

    cond do
      gated != nil ->
        "GATED (#{gated.scenario})"

      true ->
        scores = results |> Enum.map(& &1.score) |> Enum.reject(&is_nil/1)

        if scores == [] do
          "-"
        else
          Integer.to_string(round(Enum.sum(scores) / length(scores)))
        end
    end
  end

  defp label(result), do: result.label || result.scenario

  defp score_str(nil), do: "-"
  defp score_str(score), do: Integer.to_string(score)

  defp width(all, fun) do
    all |> Enum.map(fun) |> Enum.map(&String.length/1) |> Enum.max()
  end

  defp write_json!(path, value) do
    File.write!(path, JSON.encode!(json_safe(value)) <> "\n")
  end

  def json_safe(%_{} = struct) do
    case struct do
      %DateTime{} -> DateTime.to_iso8601(struct)
      %NaiveDateTime{} -> NaiveDateTime.to_iso8601(struct)
      _ -> struct |> Map.from_struct() |> json_safe()
    end
  end

  def json_safe(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), json_safe(value)} end)
  end

  def json_safe(list) when is_list(list), do: Enum.map(list, &json_safe/1)
  def json_safe(tuple) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> json_safe()

  def json_safe(atom) when is_atom(atom) and atom not in [true, false, nil],
    do: Atom.to_string(atom)

  def json_safe(value), do: value
end
