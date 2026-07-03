defmodule Docket.SchemaTest do
  use ExUnit.Case, async: true

  alias Docket.Schema

  # Minimal v1 validation engine: type checks, required object fields, enum
  # membership, unknown object key rejection. Constraints beyond these are
  # ignored in v1.

  describe "validate/2 primitives" do
    test "validates strings" do
      assert :ok = Schema.validate(Schema.string(), "hello")
      assert {:error, _reasons} = Schema.validate(Schema.string(), 42)
    end

    test "validates floats and accepts integers as numeric values" do
      assert :ok = Schema.validate(Schema.float(), 0.5)
      assert :ok = Schema.validate(Schema.float(), 3)
      assert {:error, _reasons} = Schema.validate(Schema.float(), "3.0")
    end

    test "validates maps" do
      assert :ok = Schema.validate(Schema.map(), %{"any" => "shape"})
      assert {:error, _reasons} = Schema.validate(Schema.map(), [1, 2])
    end

    test "validates enum membership" do
      schema = Schema.enum(["low", "high"])

      assert :ok = Schema.validate(schema, "low")
      assert {:error, _reasons} = Schema.validate(schema, "medium")
    end
  end

  describe "validate/2 objects" do
    setup do
      schema =
        Schema.object(%{
          "name" => Schema.string(required: true),
          "score" => Schema.float()
        })

      {:ok, schema: schema}
    end

    test "accepts values matching declared fields", %{schema: schema} do
      assert :ok = Schema.validate(schema, %{"name" => "a", "score" => 1.0})
      assert :ok = Schema.validate(schema, %{"name" => "a"})
    end

    test "rejects missing required fields", %{schema: schema} do
      assert {:error, reasons} = Schema.validate(schema, %{"score" => 1.0})
      assert Enum.any?(reasons, &(&1 =~ "name"))
    end

    test "rejects unknown fields", %{schema: schema} do
      assert {:error, reasons} = Schema.validate(schema, %{"name" => "a", "bogus" => 1})
      assert Enum.any?(reasons, &(&1 =~ "bogus"))
    end

    test "validates nested field values", %{schema: schema} do
      assert {:error, _reasons} = Schema.validate(schema, %{"name" => 42})
    end

    test "rejects non-map values" do
      assert {:error, _reasons} = Schema.validate(Schema.object(%{}), "nope")
    end
  end

  describe "validate/2 nil handling" do
    test "nil is valid unless the schema is required" do
      assert :ok = Schema.validate(Schema.string(), nil)
      assert {:error, _reasons} = Schema.validate(Schema.string(required: true), nil)
    end
  end
end
