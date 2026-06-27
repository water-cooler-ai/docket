# LLM Node Example

This example shows how a host application can implement a generic LLM node using
the planned v1 node contracts:

- `config_schema/0` declares configurable values such as model, reasoning
  effort, temperature, and prompt template.
- `ports/1` declares generic input/output ports. It can derive input ports from
  the prompt template at compile/verify time.
- `call/1` receives already-resolved `Docket.Node.Input.inputs` and returns
  generic `Docket.Node.Output.outputs`.

The graph node instance owns the app user's wiring from graph fields to generic
LLM ports. The runtime owns the final routing from returned output ports back to
graph channels.

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
      prompt_template: Docket.Schema.string(required: true)
    })
  end

  @impl true
  def ports(%{prompt_template: template}) do
    input_ports =
      template
      |> MyApp.PromptTemplate.variables()
      |> Map.new(fn variable ->
        {variable, Docket.Schema.string(required: true)}
      end)

    %Docket.Node.Ports{
      inputs: input_ports,
      outputs: %{
        "text" => Docket.Schema.string(required: true),
        "usage" => Docket.Schema.map(required: false)
      }
    }
  end

  @impl true
  def call(%Docket.Node.Input{} = input) do
    config = input.config
    rendered_prompt = MyApp.PromptTemplate.render(config.prompt_template, input.inputs)

    messages =
      [
        system_message(config.system_prompt),
        %{role: "user", content: rendered_prompt}
      ]
      |> Enum.reject(&is_nil/1)

    request = %{
      model: config.model,
      reasoning_effort: config.reasoning_effort,
      temperature: config.temperature,
      messages: messages,
      idempotency_key: input.idempotency_key
    }

    with {:ok, client} <- fetch_llm_client(input.context),
         {:ok, response} <- client.chat(request) do
      {:ok,
       %Docket.Node.Output{
         outputs: %{
           "text" => response.text,
           "usage" => response.usage
         },
         metadata: %{
           provider: response.provider,
           model: response.model,
           source_versions: input.source_versions
         }
       }}
    end
  end

  defp system_message(""), do: nil
  defp system_message(prompt), do: %{role: "system", content: prompt}

  defp fetch_llm_client(%{llm_client: client}), do: {:ok, client}
  defp fetch_llm_client(_context), do: {:error, :missing_llm_client}
end
```

`MyApp.DocketNodes.LLM` does not know graph field names such as
`"customer_message"` or `"draft_response"`. It only knows its own generic port
names such as `"message"`, `"context"`, `"text"`, and `"usage"`.

## Prompt Template Helper

This helper is intentionally small. A real app may use a richer template engine,
but the important part is that template variables become dynamic input ports.

```elixir
defmodule MyApp.PromptTemplate do
  @variable_pattern ~r/{{\s*([a-zA-Z][a-zA-Z0-9_]*)\s*}}/

  def variables(template) when is_binary(template) do
    template
    |> then(&Regex.scan(@variable_pattern, &1, capture: :all_but_first))
    |> List.flatten()
    |> Enum.uniq()
  end

  def render(template, inputs) when is_binary(template) and is_map(inputs) do
    Regex.replace(@variable_pattern, template, fn _match, name ->
      inputs
      |> Map.fetch!(name)
      |> to_string()
    end)
  end
end
```

Given this prompt:

```elixir
"Reply to {{message}} using this context: {{context}}"
```

`ports/1` exposes these input ports:

```elixir
%{
  "message" => Docket.Schema.string(required: true),
  "context" => Docket.Schema.string(required: true)
}
```

## Graph Wiring

The graph node instance stores user-selected bindings from graph fields to the
LLM node's generic ports:

```elixir
graph =
  Docket.Graph.new(id: "support-reply", name: "Support Reply")
  |> Docket.Graph.input("customer_message",
    schema: Docket.Schema.string(),
    required: true
  )
  |> Docket.Graph.field("account_context",
    schema: Docket.Schema.string(),
    reducer: Docket.Reducer.last_value()
  )
  |> Docket.Graph.field("draft_response",
    schema: Docket.Schema.string(),
    reducer: Docket.Reducer.last_value()
  )
  |> Docket.Graph.field("llm_usage",
    schema: Docket.Schema.map(),
    reducer: Docket.Reducer.last_value()
  )
  |> Docket.Graph.node("draft_reply", MyApp.DocketNodes.LLM,
    config: %{
      model: "gpt-4.1-mini",
      reasoning_effort: "medium",
      temperature: 0.3,
      system_prompt: "You write concise customer support replies.",
      prompt_template: "Reply to {{message}} using this context: {{context}}"
    },
    input_bindings: %{
      "message" => "customer_message",
      "context" => "account_context"
    },
    output_bindings: %{
      "text" => "draft_response",
      "usage" => "llm_usage"
    },
    reads: ["customer_message", "account_context"],
    writes: ["draft_response", "llm_usage"]
  )
  |> Docket.Graph.edge("$start", "draft_reply")
  |> Docket.Graph.edge("draft_reply", "$finish")
  |> Docket.Graph.output("draft_response")
```

The graph field schemas and node port schemas are both expressed with
`Docket.Schema`. The compiler can therefore validate that:

- `"customer_message"` can feed the `"message"` input port
- `"account_context"` can feed the `"context"` input port
- `"text"` can write to `"draft_response"`
- `"usage"` can write to `"llm_usage"`

The `reads` and `writes` lists remain permission and routing sets. They do not
own separate schemas.

## Runtime Input And Output

At runtime, the dispatcher resolves graph channels into generic port inputs
before calling the node:

```elixir
%Docket.Node.Input{
  node_id: "draft_reply",
  inputs: %{
    "message" => "I was charged twice for my subscription.",
    "context" => "Customer is on the Pro plan. Refund policy allows one reversal."
  },
  config: %{
    model: "gpt-4.1-mini",
    reasoning_effort: "medium",
    temperature: 0.3,
    system_prompt: "You write concise customer support replies.",
    prompt_template: "Reply to {{message}} using this context: {{context}}"
  },
  source_versions: %{
    "message" => [{"customer_message", 1}],
    "context" => [{"account_context", 4}]
  }
}
```

The node returns generic output ports:

```elixir
%Docket.Node.Output{
  outputs: %{
    "text" => "I can help with that. I found the duplicate charge...",
    "usage" => %{
      "input_tokens" => 64,
      "output_tokens" => 28
    }
  }
}
```

The runtime validates those output values against the node output port schemas,
maps them through the compiled output bindings, and creates channel writes:

```text
"text"  -> "draft_response"
"usage" -> "llm_usage"
```

## Why This Shape Matters

This keeps the LLM node reusable across many workflows:

- the node module owns provider calling logic
- the node config owns model and prompt choices
- the node ports describe generic inputs and outputs
- the graph node instance owns the user's wiring choices
- `Docket.Schema` gives the compiler and runtime a shared validation language

The same pattern works for math nodes, JSON transformation nodes, HTTP request
nodes, and any other dynamic node type whose input and output shape depends on
configuration.
