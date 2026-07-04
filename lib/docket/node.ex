defmodule Docket.Node do
  @moduledoc """
  Behaviour implemented by executable node modules.
  """

  @callback config_schema() :: Docket.Schema.t()

  @doc """
  Executes the node against its state snapshot, resolved config, and runtime
  context.

  `{:await, term()}` is reserved for post-v1 late-completion protocols; in
  v1 the dispatcher treats it as a permanent node failure.
  """
  @callback call(state :: map(), config :: map(), context :: map()) ::
              {:ok, state_update :: map()}
              | {:interrupt, Docket.Interrupt.t()}
              | {:await, term()}
              | {:error, term()}
end
