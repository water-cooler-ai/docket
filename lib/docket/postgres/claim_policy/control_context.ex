if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.ControlContext do
    @moduledoc false

    alias Docket.Postgres.{ClaimPolicy, Storage}

    @spec resolve(term(), :read | :mutate) :: {:ok, map()} | {:error, atom()}
    def resolve(%{transaction_scope: true}, :mutate), do: {:error, :transaction_context_forbidden}

    def resolve(context, mode) when mode in [:read, :mutate] do
      with {:ok, resolved} <- resolve_context(context),
           :ok <- allow_transaction(resolved.repo, mode) do
        {:ok, resolved}
      end
    end

    defp resolve_context(
           %{
             repo: repo,
             prefix: prefix,
             postgres_backend: Docket.Postgres,
             postgres_admin_identity: admin_identity,
             claim_policy: %ClaimPolicy{} = claim_policy
           } = context
         )
         when is_atom(repo) and is_binary(prefix) do
      if Storage.valid_prefix?(prefix) do
        plan = ClaimPolicy.plan_context!(%{repo: repo, prefix: prefix})

        if ClaimPolicy.admin_context?(
             claim_policy,
             admin_identity,
             repo,
             prefix,
             plan.identifiers
           ) do
          {:ok,
           %{
             repo: repo,
             prefix: prefix,
             implementation_contract: ClaimPolicy.activation_contract(claim_policy),
             transaction_scope: Map.get(context, :transaction_scope, false),
             identifiers: %{
               policy: plan.identifiers.claim_policy,
               partitions: plan.identifiers.claim_partitions,
               receipts: plan.identifiers.claim_policy_receipts,
               events: plan.identifiers.claim_policy_events,
               assertions: plan.identifiers.claim_assertions,
               rollout: plan.identifiers.claim_rollout,
               gate: plan.identifiers.claim_admission_gate,
               capabilities: plan.identifiers.claim_capabilities,
               runs: plan.identifiers.runs
             }
           }}
        else
          {:error, :invalid_admin_context}
        end
      else
        {:error, :invalid_admin_context}
      end
    rescue
      _ -> {:error, :invalid_admin_context}
    end

    defp resolve_context(_context), do: {:error, :invalid_admin_context}

    defp allow_transaction(repo, :mutate) do
      if function_exported?(repo, :in_transaction?, 0) and repo.in_transaction?(),
        do: {:error, :transaction_context_forbidden},
        else: :ok
    end

    defp allow_transaction(_repo, :read), do: :ok
  end
end
