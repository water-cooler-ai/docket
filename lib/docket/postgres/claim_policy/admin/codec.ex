if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.Admin.Codec do
    @moduledoc false

    def request_fingerprint(request), do: :crypto.hash(:sha256, canonical(request))
    def target_fingerprint(target), do: :crypto.hash(:sha256, canonical({:v1_target, target}))

    def default_fingerprint(%{
          preferred_active: preferred_active,
          max_active: max_active,
          weight: weight,
          borrowing: borrowing
        }) do
      request_fingerprint(%{
        preferred_active: preferred_active,
        max_active: max_active,
        weight: weight,
        borrowing: borrowing
      })
    end

    def deterministic_uuid(fingerprint) do
      <<a::binary-size(4), b::binary-size(2), c0::16, d0::16, e::binary-size(6), _::binary>> =
        fingerprint

      c = Bitwise.bor(Bitwise.band(c0, 0x0FFF), 0x4000)
      d = Bitwise.bor(Bitwise.band(d0, 0x3FFF), 0x8000)

      [
        Base.encode16(a),
        Base.encode16(b),
        Base.encode16(<<c::16>>),
        Base.encode16(<<d::16>>),
        Base.encode16(e)
      ]
      |> Enum.join("-")
      |> String.downcase()
    end

    def json_encode(%DateTime{} = value), do: json_encode(DateTime.to_iso8601(value))

    def json_encode(value) when is_map(value) do
      contents =
        value
        |> Enum.sort_by(fn {key, _} -> to_string(key) end)
        |> Enum.map_join(",", fn {key, item} ->
          json_encode(to_string(key)) <> ":" <> json_encode(item)
        end)

      "{" <> contents <> "}"
    end

    def json_encode(value) when is_list(value),
      do: "[" <> Enum.map_join(value, ",", &json_encode/1) <> "]"

    def json_encode(nil), do: "null"
    def json_encode(true), do: "true"
    def json_encode(false), do: "false"
    def json_encode(value) when is_atom(value), do: json_encode(Atom.to_string(value))
    def json_encode(value) when is_integer(value), do: Integer.to_string(value)
    def json_encode(value) when is_binary(value), do: "\"" <> escape_json(value, []) <> "\""

    def json_decode!(json) when is_binary(json) do
      {value, rest} = json_value(skip_json_space(json))

      case skip_json_space(rest) do
        "" -> value
        _ -> raise ArgumentError, "invalid database JSON"
      end
    end

    defp canonical(value) when is_atom(value), do: ["a", sized(Atom.to_string(value))]
    defp canonical(value) when is_binary(value), do: ["s", sized(value)]
    defp canonical(value) when is_integer(value), do: ["i", sized(Integer.to_string(value))]
    defp canonical(value) when is_boolean(value), do: canonical(Atom.to_string(value))
    defp canonical(nil), do: "n"
    defp canonical(%DateTime{} = value), do: ["d", sized(DateTime.to_iso8601(value))]

    defp canonical(value) when is_map(value) do
      pairs =
        value
        |> Enum.sort_by(fn {key, _} -> to_string(key) end)
        |> Enum.map(fn {key, item} -> [canonical(to_string(key)), canonical(item)] end)

      ["m", sized(IO.iodata_to_binary(pairs))]
    end

    defp canonical(value) when is_tuple(value), do: canonical(Tuple.to_list(value))

    defp canonical(value) when is_list(value) do
      ["l", sized(IO.iodata_to_binary(Enum.map(value, &canonical/1)))]
    end

    defp sized(value), do: [Integer.to_string(byte_size(value)), ":", value]

    defp escape_json("", acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary()
    defp escape_json(<<"\\", rest::binary>>, acc), do: escape_json(rest, ["\\\\" | acc])
    defp escape_json(<<"\"", rest::binary>>, acc), do: escape_json(rest, ["\\\"" | acc])

    defp escape_json(<<codepoint::utf8, rest::binary>>, acc) when codepoint < 32 do
      escaped =
        "\\u" <>
          (codepoint
           |> Integer.to_string(16)
           |> String.downcase()
           |> String.pad_leading(4, "0"))

      escape_json(rest, [escaped | acc])
    end

    defp escape_json(<<codepoint::utf8, rest::binary>>, acc),
      do: escape_json(rest, [<<codepoint::utf8>> | acc])

    defp json_value(<<"{", rest::binary>>), do: json_object(skip_json_space(rest), %{})
    defp json_value(<<"[", rest::binary>>), do: json_array(skip_json_space(rest), [])
    defp json_value(<<"\"", rest::binary>>), do: json_string(rest, [])
    defp json_value(<<"true", rest::binary>>), do: {true, rest}
    defp json_value(<<"false", rest::binary>>), do: {false, rest}
    defp json_value(<<"null", rest::binary>>), do: {nil, rest}

    defp json_value(binary) do
      {number, rest} = take_json_number(binary, [])
      {String.to_integer(IO.iodata_to_binary(Enum.reverse(number))), rest}
    end

    defp json_object(<<"}", rest::binary>>, object), do: {object, rest}

    defp json_object(<<"\"", rest::binary>>, object) do
      {key, after_key} = json_string(rest, [])

      case skip_json_space(after_key) do
        <<":", after_colon::binary>> ->
          {value, after_value} = json_value(skip_json_space(after_colon))
          object = Map.put(object, audit_json_key(key), value)

          case skip_json_space(after_value) do
            <<",", next::binary>> -> json_object(skip_json_space(next), object)
            <<"}", next::binary>> -> {object, next}
            _ -> raise ArgumentError, "invalid database JSON object"
          end

        _ ->
          raise ArgumentError, "invalid database JSON object"
      end
    end

    defp json_array(<<"]", rest::binary>>, values), do: {Enum.reverse(values), rest}

    defp json_array(binary, values) do
      {value, after_value} = json_value(binary)

      case skip_json_space(after_value) do
        <<",", next::binary>> -> json_array(skip_json_space(next), [value | values])
        <<"]", next::binary>> -> {Enum.reverse([value | values]), next}
        _ -> raise ArgumentError, "invalid database JSON array"
      end
    end

    defp json_string(<<"\"", rest::binary>>, acc),
      do: {IO.iodata_to_binary(Enum.reverse(acc)), rest}

    defp json_string(<<"\\\"", rest::binary>>, acc), do: json_string(rest, ["\"" | acc])
    defp json_string(<<"\\\\", rest::binary>>, acc), do: json_string(rest, ["\\" | acc])
    defp json_string(<<"\\/", rest::binary>>, acc), do: json_string(rest, ["/" | acc])
    defp json_string(<<"\\b", rest::binary>>, acc), do: json_string(rest, [<<8>> | acc])
    defp json_string(<<"\\f", rest::binary>>, acc), do: json_string(rest, [<<12>> | acc])
    defp json_string(<<"\\n", rest::binary>>, acc), do: json_string(rest, ["\n" | acc])
    defp json_string(<<"\\r", rest::binary>>, acc), do: json_string(rest, ["\r" | acc])
    defp json_string(<<"\\t", rest::binary>>, acc), do: json_string(rest, ["\t" | acc])

    defp json_string(
           <<"\\u", high_hex::binary-size(4), "\\u", low_hex::binary-size(4), rest::binary>>,
           acc
         ) do
      high = String.to_integer(high_hex, 16)
      low = String.to_integer(low_hex, 16)

      if high in 0xD800..0xDBFF and low in 0xDC00..0xDFFF do
        codepoint = 0x10000 + Bitwise.bsl(high - 0xD800, 10) + low - 0xDC00
        json_string(rest, [<<codepoint::utf8>> | acc])
      else
        append_json_codepoint(high, <<"\\u", low_hex::binary, rest::binary>>, acc)
      end
    end

    defp json_string(<<"\\u", hex::binary-size(4), rest::binary>>, acc) do
      append_json_codepoint(String.to_integer(hex, 16), rest, acc)
    end

    defp json_string(<<byte, rest::binary>>, acc), do: json_string(rest, [<<byte>> | acc])

    defp append_json_codepoint(codepoint, _rest, _acc) when codepoint in 0xD800..0xDFFF,
      do: raise(ArgumentError, "invalid database JSON surrogate")

    defp append_json_codepoint(codepoint, rest, acc),
      do: json_string(rest, [<<codepoint::utf8>> | acc])

    defp take_json_number(<<byte, rest::binary>>, acc)
         when byte in [?-, ?0, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9] do
      take_json_number(rest, [<<byte>> | acc])
    end

    defp take_json_number(_rest, []), do: raise(ArgumentError, "invalid database JSON value")
    defp take_json_number(rest, acc), do: {acc, rest}

    defp skip_json_space(<<byte, rest::binary>>) when byte in [9, 10, 13, 32],
      do: skip_json_space(rest)

    defp skip_json_space(rest), do: rest

    defp audit_json_key("preferred_active"), do: :preferred_active
    defp audit_json_key("max_active"), do: :max_active
    defp audit_json_key("weight"), do: :weight
    defp audit_json_key("borrowing"), do: :borrowing
    defp audit_json_key("policy_version"), do: :policy_version
    defp audit_json_key("partition_version"), do: :partition_version
    defp audit_json_key("scope_key"), do: :scope_key
    defp audit_json_key("admin_state"), do: :admin_state
    defp audit_json_key("admission_epoch"), do: :admission_epoch
    defp audit_json_key("partition_present"), do: :partition_present
    defp audit_json_key("initialized_at"), do: :initialized_at
    defp audit_json_key("inserted_at"), do: :inserted_at
    defp audit_json_key("updated_at"), do: :updated_at
    defp audit_json_key("first_audit_id"), do: :first_audit_id
    defp audit_json_key("last_audit_id"), do: :last_audit_id
    defp audit_json_key("reason"), do: :reason
    defp audit_json_key("through_audit_id"), do: :through_audit_id
    defp audit_json_key("export_watermark"), do: :export_watermark
    defp audit_json_key("deleted_count"), do: :deleted_count
    defp audit_json_key("last_deleted_audit_id"), do: :last_deleted_audit_id
    defp audit_json_key("actor"), do: :actor
    defp audit_json_key("source"), do: :source
    defp audit_json_key("event_id"), do: :event_id
    defp audit_json_key("created_at"), do: :created_at
    defp audit_json_key(other), do: other
  end
end
