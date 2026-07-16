if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyAdminCodecTest do
    use ExUnit.Case, async: true

    alias Docket.Postgres.ClaimPolicy.Admin.Codec

    @policy %{preferred_active: 2, max_active: 4, weight: 1, borrowing: false}

    test "v1 durable request fingerprints match golden vectors" do
      vectors = [
        {{:v1, {:put_default, @policy, 1, "billing", "evt-1"}},
         "8d3da6c4bcca2ec444a7924a5ebfc2c7981e33e4460f8d30d2e7b419e19374e2"},
        {{:v1, {:partition_change, {"", 0, :reset_override}, "ops", "tenantless-1"}},
         "795b3883fdf5157e7d5a08ce24f620898b357f01db1b14d2b75f8ec0de8746ea"},
        {{:v1,
          {:apply_partition_changes,
           [
             {"", 0, :reset_override},
             {"é", 2, {:put_state, :drain}},
             {"😀", 4, {:put_override, @policy}}
           ], "ops", "bulk-1"}},
         "e650894f02ecd396f52443cf0f6adec6cc557e7b62b979336c8390c857c2212c"},
        {{:v1, {:export_events, 42, :crypto.hash(:sha256, "location"), "ops", "export-1"}},
         "d0e1da5d343dcba51beddd27a62395df9f70cc30d696b473b9d9a2dc26bdb837"}
      ]

      for {request, expected} <- vectors do
        assert request |> Codec.request_fingerprint() |> Base.encode16(case: :lower) == expected
      end
    end

    test "actor stays outside replay identity and target hashes are domain separated" do
      request = {:v1, {:put_default, @policy, 1, "billing", "evt-1"}}
      fingerprint = Codec.request_fingerprint(request)

      assert fingerprint == Codec.request_fingerprint(request)
      refute Codec.target_fingerprint("default") == Codec.request_fingerprint("default")

      assert Base.encode16(Codec.target_fingerprint("default"), case: :lower) ==
               "6838e31da9671833b35cdc8d17c0c88a96ce7dd9f90ac32008e41c9887f8b4bb"
    end

    test "audit JSON round-trips C0 controls and non-BMP Unicode without Jason" do
      value = %{
        reason: "tab\tunit-separator-#{<<31>>} emoji 😀",
        actor: "snowman ☃",
        deleted_count: 2
      }

      encoded = Codec.json_encode(value)
      assert encoded =~ "\\u001f"
      assert Codec.json_decode!(encoded) == value

      assert Codec.json_decode!(~S({"reason":"\ud83d\ude00"})) == %{reason: "😀"}
    end
  end
end
