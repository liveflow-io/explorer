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
end
