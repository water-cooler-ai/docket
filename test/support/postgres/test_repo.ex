if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.TestRepo do
    @moduledoc false

    use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
  end

  defmodule Docket.Postgres.RunStoreTestRepo do
    @moduledoc false

    use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
  end

  defmodule Docket.Postgres.StorageTestRepo do
    @moduledoc false

    use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
  end

  defmodule Docket.Postgres.GraphStoreTestRepo do
    @moduledoc false

    use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
  end

  defmodule Docket.Postgres.LifecycleStorageTestRepo do
    @moduledoc false

    use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
  end
end
