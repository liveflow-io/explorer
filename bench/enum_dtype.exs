defmodule Explorer.EnumDtypeBenchmark do
  require Explorer.DataFrame, as: DF

  alias Explorer.Series
  alias Explorer.Shared

  def run do
    category_count = env_integer("CATEGORIES", 10_000)
    row_count = env_integer("ROWS", 50_000)
    repetitions = env_integer("REPETITIONS", 25)

    categories = categories(category_count)
    values = Stream.cycle(categories) |> Enum.take(row_count)
    public_enum_dtype = {:enum, categories}
    internal_enum_dtype = Shared.normalise_dtype!(public_enum_dtype)

    enum_df = dataframe(values, internal_enum_dtype)
    string_df = dataframe(values, :string)
    enum_right = dataframe(Enum.reverse(values), internal_enum_dtype)
    string_right = dataframe(Enum.reverse(values), :string)
    needle = hd(categories)

    IO.puts("""
    enum dtype benchmark
      categories: #{category_count}
      rows: #{row_count}
      native enum dataframe buffers (shared enum domain excluded): #{DF.estimated_size(enum_df)} bytes
      native string dataframe buffers: #{DF.estimated_size(string_df)} bytes
    """)

    report_retained_memory("construct enum dataframe", fn ->
      dataframe(values, public_enum_dtype)
    end)

    report_retained_memory("construct string dataframe", fn -> dataframe(values, :string) end)
    report_retained_memory("return enum dataframe through Task", fn -> task_return(enum_df) end)

    report_retained_memory("return string dataframe through Task", fn ->
      task_return(string_df)
    end)

    profile("normalize public enum dtype", fn ->
      repeat(repetitions, fn -> Shared.normalise_dtype!(public_enum_dtype) end)
    end)

    profile("normalize known internal enum dtype", fn ->
      repeat(repetitions, fn -> Shared.normalise_dtype!(internal_enum_dtype) end)
    end)

    profile("return enum dataframe through Task", fn ->
      repeat(repetitions, fn -> task_return(enum_df) end)
    end)

    profile("return string dataframe through Task", fn ->
      repeat(repetitions, fn -> task_return(string_df) end)
    end)

    Benchee.run(
      %{
        "construct/enum-#{category_count}-category-domain" => fn ->
          Series.from_list(values, dtype: public_enum_dtype)
        end,
        "construct/string" => fn -> Series.from_list(values, dtype: :string) end,
        "reuse-domain/enum-8-series" => fn ->
          for _ <- 1..8, do: Series.from_list(values, dtype: internal_enum_dtype)
        end,
        "reuse-domain/string-8-series" => fn ->
          for _ <- 1..8, do: Series.from_list(values, dtype: :string)
        end,
        "dtype/known-domain-normalize" => fn ->
          repeat(1_000, fn -> Shared.normalise_dtype!(internal_enum_dtype) end)
        end,
        "dtype/public-materialize" => fn -> Series.dtype(enum_df["key"]) end,
        "task/enum-dataframe" => fn -> task_return(enum_df) end,
        "task/string-dataframe" => fn -> task_return(string_df) end,
        "filter/enum" => fn -> filter(enum_df, needle) end,
        "filter/string" => fn -> filter(string_df, needle) end,
        "group/enum" => fn -> group(enum_df) end,
        "group/string" => fn -> group(string_df) end,
        "sort/enum" => fn -> DF.sort_by(enum_df, key) end,
        "sort/string" => fn -> DF.sort_by(string_df, key) end,
        "join/enum" => fn -> DF.join(enum_df, enum_right, on: "key") end,
        "join/string" => fn -> DF.join(string_df, string_right, on: "key") end,
        "concat/enum" => fn -> DF.concat_rows(enum_df, enum_right) end,
        "concat/string" => fn -> DF.concat_rows(string_df, string_right) end,
        "lazy-collect/enum" => fn -> lazy_filter_and_collect(enum_df, needle) end,
        "lazy-collect/string" => fn -> lazy_filter_and_collect(string_df, needle) end
      },
      time: env_integer("TIME", 2),
      warmup: env_integer("WARMUP", 1),
      memory_time: 0
    )
  end

  defp categories(count) do
    for i <- 1..count do
      "00000000-0000-0000-0000-#{String.pad_leading(Integer.to_string(i), 12, "0")}"
    end
  end

  defp dataframe(values, dtype) do
    DF.new(
      key: Series.from_list(values, dtype: dtype),
      value: Series.from_list(Enum.to_list(1..length(values)//1))
    )
  end

  defp task_return(df), do: Task.async(fn -> df end) |> Task.await()

  defp filter(df, needle) do
    DF.filter_with(df, fn query -> Series.equal(query["key"], needle) end)
  end

  defp group(df) do
    df
    |> DF.group_by("key")
    |> DF.summarise(total: sum(value))
  end

  defp lazy_filter_and_collect(df, needle) do
    df
    |> DF.lazy()
    |> DF.filter_with(fn query -> Series.equal(query["key"], needle) end)
    |> DF.collect()
  end

  defp repeat(count, fun) do
    for _ <- 1..count, do: fun.()
  end

  defp report_retained_memory(label, fun) do
    parent = self()

    spawn(fn ->
      :erlang.garbage_collect()
      before = process_memory()
      result = fun.()
      retained_words = :erts_debug.flat_size(result)
      :erlang.garbage_collect()
      after_run = process_memory()
      keep_alive = :erlang.phash2({result, fun})

      send(
        parent,
        {:memory_measurement, self(), before, after_run, retained_words, keep_alive}
      )
    end)
    |> then(fn pid ->
      receive do
        {:memory_measurement, ^pid, before, after_run, retained_words, _keep_alive} ->
          IO.puts(
            "#{label}: retained BEAM term #{retained_words * :erlang.system_info(:wordsize)} bytes, " <>
              "isolated process memory delta after GC #{after_run.memory - before.memory} bytes, " <>
              "heap delta #{after_run.total_heap_size - before.total_heap_size} words"
          )
      after
        60_000 -> raise "timed out measuring #{label}"
      end
    end)
  end

  defp process_memory do
    self()
    |> :erlang.process_info([:memory, :heap_size, :total_heap_size])
    |> Map.new()
  end

  defp profile(label, fun) do
    ensure_tprof!()

    {_result, {:call_memory, report}} =
      :tprof.profile(fun, %{type: :call_memory, report: :return})

    top =
      report
      |> Enum.map(fn {module, function, arity, processes} ->
        words = Enum.reduce(processes, 0, fn {_pid, _calls, words}, total -> total + words end)
        {{module, function, arity}, words}
      end)
      |> Enum.sort_by(&elem(&1, 1), :desc)
      |> Enum.take(8)

    IO.puts("\n#{label} call_memory top entries:")
    Enum.each(top, fn {mfa, words} -> IO.puts("  #{inspect(mfa)}: #{words} words") end)
  end

  defp ensure_tprof! do
    if :code.which(:tprof) == :non_existing do
      [tools_ebin] =
        Path.wildcard(Path.join([to_string(:code.root_dir()), "lib", "tools-*", "ebin"]))

      true = Code.append_path(tools_ebin)
    end
  end

  defp env_integer(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

Explorer.EnumDtypeBenchmark.run()
