defmodule Docket.Backend.ClaimPolicyAdmin do
  @moduledoc """
  Optional backend capability for max-active-run policy administration.

  Backends expose this capability through `c:Docket.Backend.claim_policy_admin/0`.
  The public `Docket` facade owns argument validation and return-value
  normalization; implementations perform only the delegated current-state
  operation against their already-resolved backend context.
  """

  @type cap :: 1..2_147_483_647
  @type owner_scope :: Docket.Backend.owner_scope()
  @type policy :: %{
          required(:max_active_runs) => cap() | nil,
          required(:version) => non_neg_integer(),
          required(:updated_at) => DateTime.t(),
          optional(atom()) => term()
        }
  @type effective_policy :: %{
          required(:owner_scope) => owner_scope(),
          required(:max_active_runs) => cap(),
          required(:source) => :default | :override,
          required(:default_version) => non_neg_integer(),
          required(:override_version) => non_neg_integer(),
          required(:queued) => non_neg_integer(),
          required(:admitted_ready) => non_neg_integer(),
          required(:admitted_claimed) => non_neg_integer(),
          required(:debt) => non_neg_integer(),
          optional(atom()) => term()
        }

  @callback get_default(Docket.Backend.ctx()) :: {:ok, policy()} | {:error, term()}

  @callback put_default(Docket.Backend.ctx(), cap(), keyword()) ::
              {:ok, policy()} | {:error, term()}

  @callback put_override(Docket.Backend.ctx(), owner_scope(), cap(), keyword()) ::
              {:ok, policy()} | {:error, term()}

  @callback reset_override(Docket.Backend.ctx(), owner_scope(), keyword()) ::
              {:ok, policy()} | {:error, term()}

  @callback get_effective(Docket.Backend.ctx(), owner_scope()) ::
              {:ok, effective_policy()} | {:error, term()}
end
