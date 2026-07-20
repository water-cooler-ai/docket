defmodule Docket.ClaimPolicyInfo do
  @moduledoc """
  Backend-neutral effective max-active-run policy and aggregate admission state.

  Counts describe logical-run admission rather than transient claim tokens.
  `queued` is due, healthy, unadmitted work; `admitted_ready` is due, healthy,
  unclaimed admitted work; `admitted_claimed` is healthy admitted work currently
  claimed; and `debt` is admitted work above the effective cap.
  """

  @enforce_keys [
    :owner_scope,
    :max_active_runs,
    :source,
    :default_version,
    :override_version,
    :queued,
    :admitted_ready,
    :admitted_claimed,
    :debt
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          owner_scope: Docket.Backend.owner_scope(),
          max_active_runs: Docket.ClaimPolicy.cap(),
          source: :default | :override,
          default_version: non_neg_integer(),
          override_version: non_neg_integer(),
          queued: non_neg_integer(),
          admitted_ready: non_neg_integer(),
          admitted_claimed: non_neg_integer(),
          debt: non_neg_integer()
        }

  @doc false
  def new(%{
        owner_scope: owner_scope,
        max_active_runs: maximum,
        source: source,
        default_version: default_version,
        override_version: override_version,
        queued: queued,
        admitted_ready: admitted_ready,
        admitted_claimed: admitted_claimed,
        debt: debt
      })
      when source in [:default, :override] and is_integer(maximum) and maximum > 0 and
             maximum <= 2_147_483_647 and is_integer(default_version) and
             default_version >= 0 and is_integer(override_version) and override_version >= 0 and
             is_integer(queued) and queued >= 0 and is_integer(admitted_ready) and
             admitted_ready >= 0 and is_integer(admitted_claimed) and admitted_claimed >= 0 and
             is_integer(debt) and debt >= 0 do
    if valid_owner_scope?(owner_scope) do
      {:ok,
       %__MODULE__{
         owner_scope: owner_scope,
         max_active_runs: maximum,
         source: source,
         default_version: default_version,
         override_version: override_version,
         queued: queued,
         admitted_ready: admitted_ready,
         admitted_claimed: admitted_claimed,
         debt: debt
       }}
    else
      :error
    end
  end

  def new(_policy), do: :error

  defp valid_owner_scope?(:tenantless), do: true

  defp valid_owner_scope?({:tenant, tenant_id}),
    do: is_binary(tenant_id) and byte_size(tenant_id) > 0

  defp valid_owner_scope?(_owner_scope), do: false
end
