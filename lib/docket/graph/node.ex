defmodule Docket.Graph.Node do
  @moduledoc """
  Editable public graph node.

  This struct is part of the canonical graph document. It names implementation
  references, branch group metadata, config, and application metadata. It is
  not the runtime node representation.
  """

  defstruct [
    :id,
    :label,
    :description,
    :implementation,
    branches: %{},
    config: %{},
    policies: %{},
    metadata: %{}
  ]

  @type implementation ::
          %{
            required(:type) => :module,
            required(:module) => module(),
            optional(:function) => atom()
          }
          | %{required(:type) => atom(), optional(String.t()) => term()}
          | nil
  @type branch_group :: [String.t()] | %{optional(String.t()) => term()}

  @type t :: %__MODULE__{
          id: String.t() | nil,
          label: String.t() | nil,
          description: String.t() | nil,
          implementation: implementation(),
          branches: %{optional(String.t()) => branch_group()},
          config: map(),
          policies: map(),
          metadata: map()
        }
end
