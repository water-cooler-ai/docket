if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.TransitionError do
    @moduledoc false

    @retryable_codes [
      :serialization_failure,
      :deadlock_detected,
      :lock_not_available,
      :query_canceled,
      :too_many_connections,
      :cannot_connect_now
    ]

    @spec normalize(term()) :: Docket.Backend.TransitionStore.error_reason()
    def normalize(%Postgrex.Error{postgres: %{code: code}}) when code in @retryable_codes,
      do: {:retryable, code}

    def normalize(%Postgrex.Error{postgres: %{code: code}}) when not is_nil(code),
      do: {:permanent, {:postgres, code}}

    def normalize(%Postgrex.Error{}), do: {:retryable, :postgres_connection}
    def normalize(%DBConnection.ConnectionError{}), do: {:retryable, :connection}

    def normalize(reason)
        when reason in [
               :not_found,
               :invalid_transition,
               :stale_checkpoint,
               :conflict,
               :event_conflict,
               :too_large
             ],
        do: reason

    def normalize({kind, _detail} = reason) when kind in [:retryable, :permanent], do: reason
    def normalize(reason), do: {:permanent, {:storage, reason}}
  end
end
