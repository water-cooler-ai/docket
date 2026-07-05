defmodule Docket.SchemaTest do
  use ExUnit.Case, async: true

  alias Docket.Schema

  # Validation engine: type checks, required object fields, enum membership,
  # unknown object key rejection (unless open), list items, and stored
  # constraint enforcement (v1.1).

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

    test "validates integers and rejects floats" do
      assert :ok = Schema.validate(Schema.integer(), 3)
      assert {:error, _reasons} = Schema.validate(Schema.integer(), 3.0)
      assert {:error, _reasons} = Schema.validate(Schema.integer(), "3")
    end

    test "validates booleans" do
      assert :ok = Schema.validate(Schema.boolean(), true)
      assert :ok = Schema.validate(Schema.boolean(), false)
      assert {:error, _reasons} = Schema.validate(Schema.boolean(), "true")
      assert {:error, _reasons} = Schema.validate(Schema.boolean(), 1)
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

  describe "validate/2 lists" do
    test "validates each element against the item schema" do
      schema = Schema.list(Schema.string())

      assert :ok = Schema.validate(schema, [])
      assert :ok = Schema.validate(schema, ["a", "b"])
      assert {:error, reasons} = Schema.validate(schema, ["a", 2])
      assert Enum.any?(reasons, &(&1 =~ "value[1]"))
    end

    test "rejects non-list values" do
      assert {:error, _reasons} = Schema.validate(Schema.list(Schema.string()), "nope")
    end

    test "validates nested list items" do
      schema = Schema.list(Schema.object(%{"id" => Schema.string(required: true)}))

      assert :ok = Schema.validate(schema, [%{"id" => "a"}])
      assert {:error, reasons} = Schema.validate(schema, [%{}])
      assert Enum.any?(reasons, &(&1 =~ "value[0].id"))
    end

    test "a hand-built list schema without an item accepts any elements" do
      assert :ok = Schema.validate(%Schema{type: :list}, ["a", 1, %{}])
    end
  end

  describe "validate/2 constraints" do
    test "enforces min/max on floats and integers" do
      assert :ok = Schema.validate(Schema.float(min: 0, max: 1), 0.5)
      assert {:error, reasons} = Schema.validate(Schema.float(min: 0), -0.5)
      assert Enum.any?(reasons, &(&1 =~ "at least 0"))

      assert :ok = Schema.validate(Schema.integer(min: 0, max: 10), 10)
      assert {:error, reasons} = Schema.validate(Schema.integer(max: 10), 11)
      assert Enum.any?(reasons, &(&1 =~ "at most 10"))
    end

    test "bounds are inclusive" do
      schema = Schema.integer(min: 1, max: 3)

      assert :ok = Schema.validate(schema, 1)
      assert :ok = Schema.validate(schema, 3)
    end

    test "enforces min_length/max_length on strings" do
      schema = Schema.string(min_length: 2, max_length: 3)

      assert :ok = Schema.validate(schema, "ab")
      assert {:error, _reasons} = Schema.validate(schema, "a")
      assert {:error, _reasons} = Schema.validate(schema, "abcd")
    end

    test "enforces pattern on strings" do
      schema = Schema.string(pattern: "^[a-z]+$")

      assert :ok = Schema.validate(schema, "abc")
      assert {:error, reasons} = Schema.validate(schema, "Abc")
      assert Enum.any?(reasons, &(&1 =~ "pattern"))
    end

    test "reports an invalid pattern constraint instead of crashing" do
      assert {:error, reasons} = Schema.validate(Schema.string(pattern: "["), "abc")
      assert Enum.any?(reasons, &(&1 =~ "invalid pattern"))
    end

    test "enforces min_items/max_items on lists" do
      schema = Schema.list(Schema.string(), min_items: 1, max_items: 2)

      assert :ok = Schema.validate(schema, ["a"])
      assert {:error, _reasons} = Schema.validate(schema, [])
      assert {:error, _reasons} = Schema.validate(schema, ["a", "b", "c"])
    end

    test "constraints load from the wire with string keys" do
      schema = %Schema{type: :string, constraints: %{"min_length" => 2}}

      assert {:error, _reasons} = Schema.validate(schema, "a")
    end

    test "hand-built atom constraint keys are still enforced" do
      schema = %Schema{type: :integer, constraints: %{min: 5}}

      assert {:error, _reasons} = Schema.validate(schema, 4)
    end

    test "unknown constraint keys are ignored" do
      assert :ok = Schema.validate(Schema.string(format: "email"), "not-an-email")
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

    test "open objects permit unknown keys but still validate declared fields" do
      schema = Schema.object(%{"name" => Schema.string(required: true)}, open: true)

      assert :ok = Schema.validate(schema, %{"name" => "a", "extra" => 1})
      assert {:error, reasons} = Schema.validate(schema, %{"extra" => 1})
      assert Enum.any?(reasons, &(&1 =~ "name"))
    end
  end

  describe "validate/2 nil handling" do
    test "nil is valid unless the schema is required" do
      assert :ok = Schema.validate(Schema.string(), nil)
      assert {:error, _reasons} = Schema.validate(Schema.string(required: true), nil)
    end
  end
end
