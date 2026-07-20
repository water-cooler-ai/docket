# LLM Node Example

A generic host-defined LLM node uses the simplified v0.1 node contract:

- `config_schema/0` declares configurable values such as model, reasoning
  effort, temperature, output field, and prompt template.
- `call/3` receives the current graph state, normalized node config, and runtime
  context.
- The node returns a partial state update: only the fields it wants to change.

The graph node instance does not store per-node input bindings, output bindings,
reads, or writes. Prompt variables are interpreted as graph state keys. If a
prompt says `{{customer_message}}`, the node reads `state["customer_message"]`.

## Node Implementation

```elixir
defmodule MyApp.DocketNodes.LLM do
  @behaviour Docket.Node

  @impl true
  def config_schema do
    Docket.Schema.object(%{
      model: Docket.Schema.string(required: true),
      reasoning_effort:
        Docket.Schema.enum(["none", "low", "medium", "high"],
          default: "medium"
        ),
      temperature:
        Docket.Schema.float(
          min: 0.0,
          max: 2.0,
          default: 0.4
        ),
      system_prompt: Docket.Schema.string(default: ""),
      prompt_template: Docket.Schema.string(required: true),
      output_field: Docket.Schema.string(required: true),
      usage_field: Docket.Schema.string(required: false)
    })
  end

  @impl true
  def call(state, config, context) do
    rendered_prompt = MyApp.PromptTemplate.render(config["prompt_template"], state)

    messages =
      [
        system_message(config["system_prompt"]),
        %{role: "user", content: rendered_prompt}
      ]
      |> Enum.reject(&is_nil/1)

    request = %{
      model: config["model"],
      reasoning_effort: config["reasoning_effort"],
      temperature: config["temperature"],
      messages: messages,
      idempotency_key: context.idempotency_key
    }

    with {:ok, client} <- fetch_llm_client(context),
         {:ok, response} <- client.chat(request) do
      update =
        %{}
        |> Map.put(config["output_field"], response.text)
        |> maybe_put(config["usage_field"], response.usage)

      {:ok, update}
    end
  end

  defp maybe_put(update, nil, _value), do: update
  defp maybe_put(update, "", _value), do: update
  defp maybe_put(update, field, value), do: Map.put(update, field, value)

  defp system_message(""), do: nil
  defp system_message(prompt), do: %{role: "system", content: prompt}

  defp fetch_llm_client(%{application: %{llm_client: client}}), do: {:ok, client}
  defp fetch_llm_client(_context), do: {:error, :missing_llm_client}
end
```

`MyApp.DocketNodes.LLM` does know graph state keys, but only because the node
instance config names them. The reusable implementation stays generic because
the prompt and output fields are configuration.

A few contract details worth noting:

- Config schemas may be declared with atom keys, but durable graph
  normalization gives `call/3` **string-keyed** config.
- App-owned context (like the LLM client) is passed as the `:context` run
  option, and arrives at the node under `context.application`. The other
  context keys (`run_id`, `node_id`, `step`, `attempt`, `source_versions`,
  `idempotency_key`) are supplied by the runtime.
- Constraints like `min`/`max` on `Docket.Schema.float/1` are stored in the
  schema but not enforced by the v0.1 validation engine.

## Prompt Template Helper

This helper is intentionally small. A real app may use a richer template engine.
The important part is that template variables are treated as state keys.

```elixir
defmodule MyApp.PromptTemplate do
  @variable_pattern ~r/{{\s*([a-zA-Z][a-zA-Z0-9_]*)\s*}}/

  def variables(template) when is_binary(template) do
    template
    |> then(&Regex.scan(@variable_pattern, &1, capture: :all_but_first))
    |> List.flatten()
    |> Enum.uniq()
  end

  def render(template, state) when is_binary(template) and is_map(state) do
    Regex.replace(@variable_pattern, template, fn _match, name ->
      state
      |> Map.fetch!(name)
      |> to_string()
    end)
  end
end
```

Given this prompt:

```elixir
"Reply to {{customer_message}} using this context: {{account_context}}"
```

the app can infer that the graph should contain state keys named
`"customer_message"` and `"account_context"`. That inference is useful for an
editor or no-code builder, but it does not need to become a separate binding
layer in the canonical graph document.

## Graph Shape

```elixir
graph =
  Docket.Graph.new!(id: "support-reply", name: "Support Reply")
  |> Docket.Graph.put_input!("customer_message",
    schema: Docket.Schema.string(),
    required: true
  )
  |> Docket.Graph.put_field!("account_context",
    schema: Docket.Schema.string(),
    reducer: Docket.Reducer.last_value()
  )
  |> Docket.Graph.put_field!("draft_response",
    schema: Docket.Schema.string(),
    reducer: Docket.Reducer.last_value()
  )
  |> Docket.Graph.put_field!("llm_usage",
    schema: Docket.Schema.map(),
    reducer: Docket.Reducer.last_value()
  )
  |> Docket.Graph.put_node!("draft_reply",
    implementation: MyApp.DocketNodes.LLM,
    config: %{
      model: "gpt-4.1-mini",
      reasoning_effort: "medium",
      temperature: 0.3,
      system_prompt: "You write concise customer support replies.",
      prompt_template:
        "Reply to {{customer_message}} using this context: {{account_context}}",
      output_field: "draft_response",
      usage_field: "llm_usage"
    }
  )
  |> Docket.Graph.put_edge!("edge_start_draft_reply", from: "$start", to: "draft_reply")
  |> Docket.Graph.put_edge!("edge_draft_reply_finish", from: "draft_reply", to: "$finish")
  |> Docket.Graph.put_output!("draft_response", [])
```

## Runtime Call

At runtime, the dispatcher calls the node with the committed state snapshot:

```elixir
state = %{
  "customer_message" => "I was charged twice for my subscription.",
  "account_context" => "Customer is on the Pro plan. Refund policy allows one reversal."
}

config = %{
  "model" => "gpt-4.1-mini",
  "reasoning_effort" => "medium",
  "temperature" => 0.3,
  "system_prompt" => "You write concise customer support replies.",
  "prompt_template" =>
    "Reply to {{customer_message}} using this context: {{account_context}}",
  "output_field" => "draft_response",
  "usage_field" => "llm_usage"
}

context = %{
  run_id: "run_123",
  node_id: "draft_reply",
  step: 1,
  attempt: 1,
  source_versions: %{"customer_message" => 1, "account_context" => 1},
  idempotency_key: "run_123:1:draft_reply:1",
  application: %{llm_client: client}
}

MyApp.DocketNodes.LLM.call(state, config, context)
```

The node returns a partial state update:

```elixir
{:ok,
 %{
   "draft_response" => "I can help with that. I found the duplicate charge...",
   "llm_usage" => %{
     "input_tokens" => 64,
     "output_tokens" => 28
   }
 }}
```

The runtime validates update keys against graph fields, validates values against
field schemas, applies reducers, and emits compiler-generated edge activations
after successful node completion.

## Why This Shape Matters

This keeps the durable graph model smaller:

- graph fields define shared state
- graph inputs seed shared state
- graph outputs project shared state
- node config binds generic implementations to graph-specific state keys
- node returns are ordinary partial state updates

No-code tooling can still parse prompt variables, suggest missing fields, and
warn about deleted fields. Those are editor concerns over the graph state model,
not a second canonical input/output binding model.
