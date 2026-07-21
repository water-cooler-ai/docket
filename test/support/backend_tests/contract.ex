defmodule Docket.BackendTests.Contract do
  @moduledoc false

  @capabilities [
    graphs: Docket.Backend.GraphStore,
    runs: Docket.Backend.RunStore,
    events: Docket.Backend.EventStore,
    claim_policy_admin: Docket.Backend.ClaimPolicyAdmin
  ]

  @spec violations(module()) :: [String.t()]
  def violations(backend) when is_atom(backend) do
    backend_violations(backend) ++ capability_violations(backend)
  end

  defp backend_violations(backend) do
    required_callbacks(Docket.Backend)
    |> Enum.flat_map(fn {name, arity} ->
      if exported?(backend, name, arity) do
        []
      else
        [
          "backend #{inspect(backend)} missing " <>
            "Docket.Backend.#{name}/#{arity}"
        ]
      end
    end)
  end

  defp capability_violations(backend) do
    Enum.flat_map(@capabilities, fn {accessor, behaviour} ->
      if exported?(backend, accessor, 0) do
        capability = apply(backend, accessor, [])

        cond do
          not is_atom(capability) ->
            [
              "backend #{inspect(backend)} #{accessor}/0 returned non-module " <>
                inspect(capability)
            ]

          true ->
            required_callbacks(behaviour)
            |> Enum.flat_map(fn {name, arity} ->
              if exported?(capability, name, arity) do
                []
              else
                [
                  "backend #{inspect(backend)} #{accessor}/0 -> #{inspect(capability)}: " <>
                    "missing #{inspect(behaviour)}.#{name}/#{arity}"
                ]
              end
            end)
        end
      else
        []
      end
    end)
  end

  defp required_callbacks(behaviour) do
    optional = behaviour.behaviour_info(:optional_callbacks)
    behaviour.behaviour_info(:callbacks) -- optional
  end

  defp exported?(module, name, arity) do
    Code.ensure_loaded?(module) and function_exported?(module, name, arity)
  end
end
