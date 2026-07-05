defmodule Docket.ReducerTest do
  use ExUnit.Case, async: true

  alias Docket.{Reducer, Schema}

  describe "reduce/3 last_value and first_value" do
    test "last_value takes the last write and ignores the prior value" do
      assert Reducer.reduce(Reducer.last_value(), {:ok, "old"}, ["a", "b"]) == "b"
      assert Reducer.reduce(nil, {:ok, "old"}, ["a", "b"]) == "b"
    end

    test "first_value keeps the prior value once set" do
      assert Reducer.reduce(Reducer.first_value(), {:ok, "kept"}, ["a", "b"]) == "kept"
      assert Reducer.reduce(Reducer.first_value(), :unset, ["a", "b"]) == "a"
    end
  end

  describe "reduce/3 append" do
    test "appends scalar writes and concatenates list writes" do
      reducer = Reducer.append()

      assert Reducer.reduce(reducer, {:ok, [1]}, [2, [3, 4]]) == [1, 2, 3, 4]
      assert Reducer.reduce(reducer, :unset, ["a"]) == ["a"]
    end

    test "nil writes append as elements" do
      assert Reducer.reduce(Reducer.append(), {:ok, [1]}, [nil]) == [1, nil]
    end

    test "unique drops duplicates keeping the first occurrence" do
      reducer = Reducer.append(unique: true)

      assert Reducer.reduce(reducer, {:ok, [1, 2]}, [2, 3, 1]) == [1, 2, 3]
    end

    test "max_length keeps the last n elements" do
      reducer = Reducer.append(max_length: 3)

      assert Reducer.reduce(reducer, {:ok, [1, 2, 3]}, [4, 5]) == [3, 4, 5]
    end
  end

  describe "reduce/3 union" do
    test "dedupes whole values keeping the first occurrence" do
      assert Reducer.reduce(Reducer.union(), {:ok, [1, 2]}, [2, 3]) == [1, 2, 3]
    end

    test "by key dedupes elements on the key value" do
      reducer = Reducer.union(by: "id")
      current = {:ok, [%{"id" => "a", "v" => 1}]}
      writes = [%{"id" => "a", "v" => 2}, %{"id" => "b", "v" => 3}]

      assert Reducer.reduce(reducer, current, writes) == [
               %{"id" => "a", "v" => 1},
               %{"id" => "b", "v" => 3}
             ]
    end

    test "elements missing the key dedupe by whole value" do
      reducer = Reducer.union(by: "id")

      assert Reducer.reduce(reducer, {:ok, ["x"]}, ["x", "y"]) == ["x", "y"]
    end
  end

  describe "reduce/3 merge" do
    test "folds writes into the prior map in order" do
      reducer = Reducer.merge()
      writes = [%{"a" => 1}, %{"a" => 2, "b" => 3}]

      assert Reducer.reduce(reducer, {:ok, %{"z" => 0}}, writes) ==
               %{"z" => 0, "a" => 2, "b" => 3}
    end

    test "shallow merge replaces nested maps; deep merge folds them" do
      current = {:ok, %{"nested" => %{"keep" => 1}}}
      writes = [%{"nested" => %{"add" => 2}}]

      assert Reducer.reduce(Reducer.merge(), current, writes) ==
               %{"nested" => %{"add" => 2}}

      assert Reducer.reduce(Reducer.merge(deep: true), current, writes) ==
               %{"nested" => %{"keep" => 1, "add" => 2}}
    end
  end

  describe "reduce/3 sum" do
    test "accumulates onto the prior value" do
      assert Reducer.reduce(Reducer.sum(), {:ok, 10}, [1, 2]) == 13
      assert Reducer.reduce(Reducer.sum(), :unset, [1.5, 2]) == 3.5
    end
  end

  describe "zero/1" do
    test "accumulating reducers have a natural zero" do
      assert Reducer.zero(Reducer.append()) == []
      assert Reducer.zero(Reducer.union()) == []
      assert Reducer.zero(Reducer.merge()) == %{}
      assert Reducer.zero(Reducer.sum()) == 0
      assert Reducer.zero(Reducer.last_value()) == nil
      assert Reducer.zero(Reducer.first_value()) == nil
      assert Reducer.zero(nil) == nil
    end
  end

  describe "write_schema/3" do
    test "append writes validate against the item; list writes per element" do
      field = Schema.list(Schema.string())

      assert %Schema{type: :string} = Reducer.write_schema(Reducer.append(), field, "x")

      assert %Schema{type: :list, item: %Schema{type: :string}} =
               Reducer.write_schema(Reducer.append(), field, ["x"])
    end

    test "merge writes against objects relax top-level required fields" do
      field =
        Schema.object(%{
          "name" => Schema.string(required: true),
          "age" => Schema.integer()
        })

      write_schema = Reducer.write_schema(Reducer.merge(), field, %{"age" => 3})

      assert :ok = Schema.validate(write_schema, %{"age" => 3})
      assert {:error, _reasons} = Schema.validate(write_schema, %{"bogus" => 1})
      assert {:error, _reasons} = Schema.validate(write_schema, nil)
    end

    test "sum writes validate as a bare number of the schema's type" do
      write_schema = Reducer.write_schema(Reducer.sum(), Schema.integer(min: 0), 5)

      assert :ok = Schema.validate(write_schema, -5)
      assert {:error, _reasons} = Schema.validate(write_schema, nil)
      assert {:error, _reasons} = Schema.validate(write_schema, "5")
    end

    test "last_value and first_value validate against the field schema" do
      field = Schema.string()

      assert Reducer.write_schema(Reducer.last_value(), field, "x") == field
      assert Reducer.write_schema(nil, field, "x") == field
    end

    test "no field schema means no write validation" do
      assert Reducer.write_schema(Reducer.append(), nil, "x") == nil
    end
  end
end
