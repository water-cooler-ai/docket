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

    rows =
      Enum.map(results, fn r -> {r.metric, label(r), score_str(r.score), evidence(r)} end)

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
    scenarios = Enum.count(results, fn r -> r.invariants != [] or invariant_failed?(r) end)
    status = if Enum.any?(results, &invariant_failed?/1), do: "FAIL", else: "PASS"

    String.pad_trailing("Invariants", w1) <>
      "  " <>
      String.pad_trailing("#{total} checks / #{scenarios} scenarios", w2) <>
      "  " <> String.pad_leading(status, w3)
  end

  defp invariant_failed?(result) do
    Enum.any?(result.invariants, &(not &1.pass)) or
      (result.passed == false and result.invariants == [])
  end

  defp overall_line(results, {w1, w2, w3}) do
    String.pad_trailing("Overall", w1) <>
      "  " <> String.pad_trailing("", w2) <> "  " <> String.pad_leading(overall(results), w3)
  end

  defp overall(results) do
    case Enum.find(results, &gated?/1) do
      nil -> composite(results)
      gated -> "GATED (#{gated.scenario})"
    end
  end

  defp gated?(result), do: result.passed == false or is_nil(result.score)

  defp composite(results) do
    metric_scores =
      results
      |> Enum.group_by(& &1.metric)
      |> Enum.map(fn {_metric, group} ->
        group |> Enum.map(& &1.score) |> Enum.reject(&is_nil/1)
      end)
      |> Enum.reject(&(&1 == []))
      |> Enum.map(fn scores -> Enum.sum(scores) / length(scores) end)

    if metric_scores == [] do
      "-"
    else
      Integer.to_string(round(Enum.sum(metric_scores) / length(metric_scores)))
    end
  end

  defp evidence(result) do
    text = result.evidence

    if String.length(text) > 160 do
      String.slice(text, 0, 157) <> "..."
    else
      text
    end
  end

  defp label(result) do
    base = result.label || result.scenario

    case Map.get(result, :policy) do
      nil -> base
      policy -> "#{base} [#{policy}]"
    end
  end

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

  def json_safe(list) when is_list(list) do
    if list != [] and Keyword.keyword?(list) do
      Map.new(list, fn {key, value} -> {to_string(key), json_safe(value)} end)
    else
      Enum.map(list, &json_safe/1)
    end
  end

  def json_safe(tuple) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> json_safe()

  def json_safe(atom) when is_atom(atom) and atom not in [true, false, nil],
    do: Atom.to_string(atom)

  def json_safe(value), do: value
end
