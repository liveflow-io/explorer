defmodule Explorer.EnumDomainTest do
  use ExUnit.Case, async: true

  require Explorer.DataFrame, as: DF
  alias Explorer.Series
  alias Explorer.Shared

  @categories ["open", "pending", "closed"]
  @dtype {:enum, @categories}

  test "keeps enum domains compact internally and materializes the public dtype" do
    series = Series.from_list(["open", nil], dtype: @dtype)
    df = DF.new(status: series)

    assert {:enum, %Explorer.EnumDomain{size: 3}} = series.dtype
    assert {:enum, %Explorer.EnumDomain{size: 3}} = df.dtypes["status"]
    assert Series.dtype(series) == @dtype
    assert DF.dtypes(df) == %{"status" => @dtype}
    assert Series.categories(series) |> Series.to_list() == @categories
  end

  test "normalizing an internal domain does not rebuild it" do
    internal = Shared.normalise_dtype!(@dtype)

    assert Shared.normalise_dtype!(internal) === internal
  end

  test "casting to a separately normalized matching domain is a no-op" do
    series = Series.from_list(["open", "closed"], dtype: @dtype)

    assert Series.cast(series, @dtype) === series
  end

  test "compares separately created identical domains without relying on fingerprints" do
    left = Shared.normalise_dtype!(@dtype)
    right = Shared.normalise_dtype!(@dtype)
    reordered = Shared.normalise_dtype!({:enum, Enum.reverse(@categories)})

    assert Shared.dtype_equal?(left, right)
    refute Shared.dtype_equal?(left, reordered)
  end

  test "rejects duplicate and non-string categories" do
    assert_raise ArgumentError, fn ->
      Series.from_list([], dtype: {:enum, ["open", "open"]})
    end

    assert_raise ArgumentError, fn ->
      Series.from_list([], dtype: {:enum, ["open", :closed]})
    end
  end

  test "creates identical domains concurrently" do
    domains =
      1..50
      |> Task.async_stream(fn _ -> Shared.normalise_dtype!(@dtype) end,
        ordered: false,
        max_concurrency: 10,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, domain} -> domain end)

    assert Enum.all?(domains, &Shared.dtype_equal?(hd(domains), &1))
  end

  test "preserves equality after bounded cache churn" do
    domain = Shared.normalise_dtype!(@dtype)

    _other_domains =
      for i <- 1..129 do
        Shared.normalise_dtype!({:enum, ["domain-#{i}"]})
      end

    assert Shared.dtype_equal?(domain, Shared.normalise_dtype!(@dtype))
  end

  test "retains declared domains for empty and nil-only series" do
    for values <- [[], [nil, nil]] do
      series = Series.from_list(values, dtype: @dtype)

      assert Series.dtype(series) == @dtype
      assert Series.categories(series) |> Series.to_list() == @categories
    end

    empty_domain = Series.from_list([], dtype: {:enum, []})
    assert Series.dtype(empty_domain) == {:enum, []}
    assert Series.categories(empty_domain) |> Series.to_list() == []
  end

  test "materializes nested enum dtypes" do
    dtype = {:list, @dtype}
    series = Series.from_list([["open", "closed"], nil], dtype: dtype)

    assert Series.dtype(series) == dtype
    assert Series.to_list(series) == [["open", "closed"], nil]
  end

  test "keeps separately created matching domains join and concat compatible" do
    left_key = Series.from_list(["open", "closed"], dtype: @dtype)
    right_key = Series.from_list(["closed", "open"], dtype: @dtype)
    left = DF.new(status: left_key, left_value: Series.from_list([1, 2]))
    right = DF.new(status: right_key, right_value: Series.from_list([3, 4]))

    joined = DF.join(left, right, on: "status") |> DF.sort_by(left_value)

    concatenated =
      DF.concat_rows(left, DF.new(status: right_key, left_value: Series.from_list([3, 4])))

    assert DF.to_columns(joined, atom_keys: true) == %{
             status: ["open", "closed"],
             left_value: [1, 2],
             right_value: [4, 3]
           }

    assert DF.to_columns(concatenated, atom_keys: true).status ==
             ["open", "closed", "closed", "open"]

    assert DF.dtypes(joined)["status"] == @dtype
    assert DF.dtypes(concatenated)["status"] == @dtype
  end

  test "keeps separately created matching domains select and coalesce compatible" do
    left = Series.from_list(["open", nil], dtype: @dtype)
    right = Series.from_list(["pending", "closed"], dtype: @dtype)
    predicate = Series.from_list([true, false])

    selected = Series.select(predicate, left, right)
    coalesced = Series.coalesce(left, right)

    assert Series.to_list(selected) == ["open", "closed"]
    assert Series.to_list(coalesced) == ["open", "closed"]
    assert Series.dtype(selected) == @dtype
    assert Series.dtype(coalesced) == @dtype
  end

  test "pivots separately created matching enum domains" do
    df =
      DF.new(
        left: Series.from_list(["open", "closed"], dtype: @dtype),
        right: Series.from_list(["pending", "open"], dtype: @dtype)
      )

    pivoted = DF.pivot_longer(df, ["left", "right"], select: [])

    assert DF.dtypes(pivoted) == %{"variable" => :string, "value" => @dtype}

    assert DF.to_columns(pivoted, atom_keys: true).value ==
             ["open", "closed", "pending", "open"]
  end

  test "keeps enum domains through lazy expressions and collection" do
    df =
      DF.new(
        status: Series.from_list(["closed", "open", "pending"], dtype: @dtype),
        value: Series.from_list([3, 1, 2])
      )

    result =
      df
      |> DF.lazy()
      |> DF.filter_with(fn ldf -> Series.not_equal(ldf["status"], "closed") end)
      |> DF.sort_by(value)
      |> DF.collect()

    assert DF.to_columns(result, atom_keys: true) == %{
             status: ["open", "pending"],
             value: [1, 2]
           }

    assert DF.dtypes(result)["status"] == @dtype
  end

  test "keeps native enum domains alive when a task owner exits" do
    df =
      Task.async(fn ->
        DF.new(status: Series.from_list(["open", "closed"], dtype: @dtype))
      end)
      |> Task.await(30_000)

    assert DF.to_columns(df, atom_keys: true) == %{status: ["open", "closed"]}
    assert DF.dtypes(df)["status"] == @dtype
  end

  test "BEAM dataframe metadata size does not scale with enum cardinality" do
    dataframe_size = fn count ->
      categories = Enum.map(1..count, &Integer.to_string/1)
      df = DF.new(value: Series.from_list([], dtype: {:enum, categories}))
      :erts_debug.flat_size(df)
    end

    assert dataframe_size.(10_000) == dataframe_size.(10)
  end

  describe "domains as category collections" do
    test "enumerates the categories held by the internal dtype" do
      {:enum, domain} = Series.from_list(["open"], dtype: @dtype).dtype

      assert Enum.to_list(domain) == @categories
      assert Enum.count(domain) == 3
      assert Enum.map(domain, &String.upcase/1) == ["OPEN", "PENDING", "CLOSED"]
    end

    test "answers membership without materializing the categories" do
      {:enum, domain} = Series.from_list(["open"], dtype: @dtype).dtype

      assert "pending" in domain
      refute "missing" in domain
      refute :pending in domain
    end

    test "reports categories rather than the domain struct in dtype errors" do
      series = Series.from_list(["open"], dtype: @dtype)

      assert_raise ArgumentError,
                   ~r/mismatched dtypes: \{:enum, \["open", "pending", "closed"\]\} and \{:s, 64\}/,
                   fn -> Series.coalesce(series, Series.from_list([1])) end
    end
  end

  describe "dtypes read back through Series.dtype/1" do
    test "build series that compare against the series they came from" do
      series = Series.from_list(["open", "closed"], dtype: @dtype)
      other = Series.from_list(["open", "pending"], dtype: Series.dtype(series))

      assert Series.to_list(Series.equal(series, other)) == [true, false]
      assert Series.to_list(Series.not_equal(series, other)) == [false, true]
      assert Series.to_list(Series.in(series, other)) == [true, false]
    end

    test "round-trip through nested dtypes" do
      series = Series.from_list([["open"], ["closed"]], dtype: {:list, @dtype})
      other = Series.from_list([["open"], ["pending"]], dtype: Series.dtype(series))

      assert Series.to_list(Series.equal(series, other)) == [true, false]
    end

    test "still reject genuinely different domains" do
      series = Series.from_list(["open"], dtype: @dtype)
      other = Series.from_list(["a"], dtype: {:enum, ["a"]})

      assert_raise ArgumentError, ~r/mismatched dtypes/, fn -> Series.equal(series, other) end
    end
  end

  describe "fill_missing/2" do
    test "fills with a value from the domain and keeps the dtype" do
      series = Series.from_list(["open", nil], dtype: @dtype)
      filled = Series.fill_missing(series, "closed")

      assert Series.dtype(filled) == @dtype
      assert Series.to_list(filled) == ["open", "closed"]
    end

    test "raises naming the value and the domain when it is outside the categories" do
      series = Series.from_list(["open", nil], dtype: @dtype)

      assert_raise ArgumentError,
                   ~s(cannot fill missing values with "archived" because it is not one of the ) <>
                     ~s(categories of {:enum, ["open", "pending", "closed"]}),
                   fn -> Series.fill_missing(series, "archived") end
    end

    test "supports the ordered strategies" do
      series = Series.from_list(["pending", nil, "open"], dtype: @dtype)

      for {strategy, expected} <- [
            forward: ["pending", "pending", "open"],
            backward: ["pending", "open", "open"],
            min: ["pending", "open", "open"],
            max: ["pending", "pending", "open"]
          ] do
        filled = Series.fill_missing(series, strategy)

        assert Series.dtype(filled) == @dtype
        assert Series.to_list(filled) == expected
      end
    end

    test "supports strategies on lists of enums" do
      series = Series.from_list([["open"], nil, ["closed"]], dtype: {:list, @dtype})
      filled = Series.fill_missing(series, :forward)

      assert Series.dtype(filled) == {:list, @dtype}
      assert Series.to_list(filled) == [["open"], ["open"], ["closed"]]
    end

    test "fills inside a query" do
      df = DF.new(status: Series.from_list(["open", nil], dtype: @dtype))

      out =
        DF.mutate_with(df, fn ldf -> [status: Series.fill_missing(ldf["status"], "closed")] end)

      assert DF.dtypes(out) == %{"status" => @dtype}
      assert DF.to_columns(out, atom_keys: true) == %{status: ["open", "closed"]}
    end
  end

  describe "supertyping enums with strings" do
    test "select/3 keeps the enum when the string side fits the domain" do
      series = Series.from_list(["open", nil], dtype: @dtype)
      selected = Series.select(Series.is_nil(series), "closed", series)

      assert Series.dtype(selected) == @dtype
      assert Series.to_list(selected) == ["open", "closed"]
    end

    test "select/3 raises rather than widening when the value is outside the domain" do
      series = Series.from_list(["open", nil], dtype: @dtype)

      assert_raise RuntimeError, ~r/archived/, fn ->
        Series.select(Series.is_nil(series), "archived", series)
      end
    end

    test "select/3 supertypes with the enum on either branch" do
      series = Series.from_list(["open", nil], dtype: @dtype)
      predicate = Series.is_nil(series)
      strings = Series.from_list(["closed", "closed"])

      assert Series.dtype(Series.select(predicate, strings, series)) == @dtype
      assert Series.dtype(Series.select(predicate, series, strings)) == @dtype
    end

    test "coalesce/2 keeps the enum when the string side fits the domain" do
      series = Series.from_list(["open", nil], dtype: @dtype)
      coalesced = Series.coalesce(series, Series.from_list(["pending", "closed"]))

      assert Series.dtype(coalesced) == @dtype
      assert Series.to_list(coalesced) == ["open", "closed"]
    end

    test "coalesce/2 raises rather than widening when the string side leaves the domain" do
      series = Series.from_list(["open", nil], dtype: @dtype)

      assert_raise RuntimeError, ~r/archived/, fn ->
        Series.coalesce(series, Series.from_list(["pending", "archived"]))
      end
    end

    test "supertypes a string scalar inside a query" do
      df = DF.new(status: Series.from_list(["open", nil], dtype: @dtype))

      out =
        DF.mutate_with(df, fn ldf ->
          [status: Series.select(Series.is_nil(ldf["status"]), "closed", ldf["status"])]
        end)

      assert DF.dtypes(out) == %{"status" => @dtype}
      assert DF.to_columns(out, atom_keys: true) == %{status: ["open", "closed"]}
    end

    test "keeps the enum when the string side is a lazy column" do
      df =
        DF.new(
          status: Series.from_list(["open", nil], dtype: @dtype),
          fallback: Series.from_list([nil, "closed"])
        )

      out =
        DF.mutate_with(df, fn ldf -> [result: Series.coalesce(ldf["status"], ldf["fallback"])] end)

      assert DF.dtypes(out)["result"] == @dtype
      assert DF.to_columns(out, atom_keys: true).result == ["open", "closed"]
    end

    test "raises when a lazy string column leaves the domain" do
      df =
        DF.new(
          status: Series.from_list(["open", nil], dtype: @dtype),
          fallback: Series.from_list([nil, "archived"])
        )

      assert_raise RuntimeError, ~r/archived/, fn ->
        DF.mutate_with(df, fn ldf -> [result: Series.coalesce(ldf["status"], ldf["fallback"])] end)
      end
    end

    test "still raises for dtypes that have no supertype" do
      series = Series.from_list(["open"], dtype: @dtype)

      assert_raise ArgumentError, ~r/mismatched dtypes/, fn ->
        Series.select(Series.from_list([true]), series, Series.from_list([1]))
      end
    end
  end

  describe "membership with values outside the domain" do
    test "does not match instead of failing an eager series" do
      series = Series.from_list(["open", "closed"], dtype: @dtype)

      assert Series.to_list(Series.in(series, Series.from_list(["archived"]))) == [false, false]
      assert Series.to_list(Series.in(series, ["closed", "archived"])) == [false, true]
    end

    test "does not match instead of failing a query" do
      df = DF.new(status: Series.from_list(["open", "closed"], dtype: @dtype))

      unknown =
        DF.filter_with(df, fn ldf -> Series.in(ldf["status"], Series.from_list(["archived"])) end)

      mixed =
        DF.filter_with(df, fn ldf ->
          Series.in(ldf["status"], Series.from_list(["closed", "archived"]))
        end)

      assert DF.to_columns(unknown, atom_keys: true) == %{status: []}
      assert DF.to_columns(mixed, atom_keys: true) == %{status: ["closed"]}
    end

    test "does not match through the `not in` operator" do
      df = DF.new(status: Series.from_list(["open", "closed"], dtype: @dtype))
      unknown = Series.from_list(["archived"])

      assert DF.to_columns(DF.filter(df, status not in ^unknown), atom_keys: true) == %{
               status: ["open", "closed"]
             }
    end

    test "does not match a list of enums, keeping nils" do
      series = Series.from_list([["open"], nil, []], dtype: {:list, @dtype})

      assert Series.to_list(Series.member?(series, "archived")) == [false, nil, false]
      assert Series.to_list(Series.member?(series, "open")) == [true, nil, false]
    end

    test "does not match a list of enums inside a query" do
      df = DF.new(status: Series.from_list([["open"], nil, []], dtype: {:list, @dtype}))
      out = DF.mutate_with(df, fn ldf -> [found: Series.member?(ldf["status"], "archived")] end)

      assert DF.to_columns(out, atom_keys: true).found == [false, nil, false]
    end
  end
end
