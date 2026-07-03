defmodule Docket.Node do
  @moduledoc """
  Behaviour implemented by executable node modules.
  """

  @callback config_schema() :: Docket.Schema.t()
  @callback call(state :: map(), config :: map(), context :: map()) ::
              {:ok, state_update :: map()}
              | {:interrupt, term()}
              | {:await, term()}
              | {:error, term()}
end
