defmodule Nebulex.MultilevelQueryableTest do
  import Nebulex.CacheCase

  deftests do
    import Nebulex.CacheCase

    describe "get_all!/2" do
      test "ok: matches all cached entries", %{cache: cache} do
        set1 = cache_put(cache, 1..50)
        set2 = cache_put(cache, 51..100)

        for x <- 1..100, do: assert(cache.fetch!(x) == x)

        expected = set1 ++ set2

        assert cache.get_all!() |> :lists.usort() == Enum.zip([expected, expected])
        assert cache.get_all!(select: :key) |> :lists.usort() == expected
        assert cache.get_all!(select: :value) |> :lists.usort() == expected

        set3 = Enum.to_list(20..60)
        :ok = Enum.each(set3, &cache.delete!(&1))
        expected = :lists.usort(expected -- set3)

        assert cache.get_all!() |> :lists.usort() == Enum.zip([expected, expected])
        assert cache.get_all!(select: :key) |> :lists.usort() == expected
        assert cache.get_all!(select: :value) |> :lists.usort() == expected
      end

      test "ok: returns an empty list when the cache is empty", %{cache: cache} do
        assert cache.get_all!() == []
      end

      test "error: invalid option value for query spec", %{cache: cache} do
        for opt <- [:select, :in] do
          msg = ~r"invalid value for #{inspect(opt)} option"

          assert_raise NimbleOptions.ValidationError, msg, fn ->
            cache.get_all([{opt, :invalid}])
          end
        end
      end

      test "error: unknown option in query spec", %{cache: cache} do
        assert_raise NimbleOptions.ValidationError, ~r"unknown options", fn ->
          cache.get_all(foo: :bar)
        end
      end

      test "error: invalid option entry for query spec", %{cache: cache} do
        assert_raise ArgumentError, ~r"expected a keyword list, but an entry in the list", fn ->
          cache.get_all([:invalid])
        end
      end

      test "error: invalid query spec", %{cache: cache} do
        msg = ~r"invalid query spec: expected a keyword list, got: :invalid"

        assert_raise ArgumentError, msg, fn ->
          cache.get_all(:invalid)
        end
      end
    end

    describe "stream!/2" do
      @entries for x <- 1..10, into: %{}, do: {x, x * 2}

      test "ok: returns all keys in cache", %{cache: cache} do
        :ok = cache.put_all(@entries)

        assert cache.stream!(select: :key)
               |> Enum.to_list()
               |> :lists.usort() == Map.keys(@entries)
      end

      test "ok: returns all values in cache", %{cache: cache} do
        :ok = cache.put_all(@entries)

        assert [select: :value]
               |> cache.stream!(max_entries: 2)
               |> Enum.to_list()
               |> :lists.usort() == Map.values(@entries)
      end

      test "ok: returns all key/value pairs in cache", %{cache: cache} do
        :ok = cache.put_all(@entries)

        assert [select: {:key, :value}]
               |> cache.stream!(max_entries: 2)
               |> Enum.to_list()
               |> :lists.usort() == Map.to_list(@entries)
      end

      test "ok: returns an empty list when the cache is empty", %{cache: cache} do
        assert cache.stream!() |> Enum.to_list() == []
      end
    end

    describe "delete_all!/2" do
      test "ok: evicts all entries in the cache", %{cache: cache} do
        Enum.each(1..2, fn _ ->
          entries = cache_put(cache, 1..50)

          assert cache.get_all!() |> :lists.usort() |> length() == length(entries)

          cached = cache.count_all!()
          assert cache.delete_all!() == cached
          assert cache.count_all!() == 0
        end)
      end

      test "ok: deleted count is 0 when the cache is empty", %{cache: cache} do
        assert cache.delete_all!() == 0
      end

      test "ok: empty list or map has not any effect", %{cache: cache} do
        assert cache.put_all([]) == :ok
        assert cache.put_all(%{}) == :ok

        assert cache.delete_all!() == 0
      end
    end

    describe "count_all!/2" do
      test "ok: returns the total number of cached entries", %{cache: cache} do
        for x <- 1..100, do: cache.put(x, x)

        # total = keys per level * 3 levels
        total = cache.get_all!() |> length() |> Kernel.*(3)

        assert cache.count_all!() == total

        for x <- 1..50, do: cache.delete!(x)

        total = cache.get_all!() |> length() |> Kernel.*(3)
        assert cache.count_all!() == total

        for x <- 51..60, do: assert(cache.fetch!(x) == x)
      end

      test "ok: count is 0 when the cache is empty", %{cache: cache} do
        assert cache.count_all!() == 0
      end

      test "ok: empty list or map has not any effect", %{cache: cache} do
        assert cache.put_all([]) == :ok
        assert cache.put_all(%{}) == :ok

        assert cache.count_all!() == 0
      end
    end

    describe "get_all!/2 - [in: keys]" do
      test "ok: returns the entries associated to the requested keys", %{cache: cache} do
        assert cache.put_all(a: 1, c: 3) == :ok

        keys = [:a, :b, :c]

        assert cache.get_all!(in: keys) |> Map.new() == %{a: 1, c: 3}
        assert cache.get_all!(in: keys, select: :key) |> :lists.usort() == [:a, :c]
        assert cache.get_all!(in: keys, select: :value) |> :lists.usort() == [1, 3]

        # 2 keys per level * 3 levels = 6
        assert cache.delete_all!() == 6
      end

      test "ok: returns an empty list when none of the given keys is in cache", %{cache: cache} do
        assert cache.get_all!(in: ["foo", "bar", 1, :a]) == []
      end

      test "ok: returns an empty list when the given key list is empty", %{cache: cache} do
        assert cache.get_all!(in: []) == []
      end

      test "error: raises an exception because invalid query spec", %{cache: cache} do
        assert_raise NimbleOptions.ValidationError, ~r"invalid value for :in option", fn ->
          cache.get_all!(in: :invalid)
        end
      end
    end

    describe "stream!/2 - [in: keys]" do
      test "ok: returns the entries associated to the requested keys", %{cache: cache} do
        entries = for x <- 1..10, into: %{}, do: {x, x * 2}
        assert cache.put_all(entries) == :ok

        keys = [1, 2, 3, 4, 5, 11, 100]
        expected_keys = Map.take(entries, keys) |> Map.keys()
        expected_values = Map.take(entries, keys) |> Map.values()

        assert cache.stream!(in: keys) |> Map.new() == Map.take(entries, keys)

        assert cache.stream!(in: keys, select: :key) |> Enum.to_list() |> :lists.usort() ==
                 expected_keys

        assert cache.stream!(in: keys, select: :value) |> Enum.to_list() |> :lists.usort() ==
                 expected_values

        # 10 keys per level * 3 levels = 30
        assert cache.delete_all!() == 30
      end

      test "ok: returns an empty list when none of the given keys is in cache", %{cache: cache} do
        assert cache.stream!(in: ["foo", "bar", 1, :a]) |> Enum.to_list() == []
      end

      test "ok: returns an empty list when the given key list is empty", %{cache: cache} do
        assert cache.stream!(in: []) |> Enum.to_list() == []
      end

      test "error: raises an exception because invalid query spec", %{cache: cache} do
        assert_raise NimbleOptions.ValidationError, ~r"invalid value for :in option", fn ->
          cache.stream!(in: :invalid)
        end
      end
    end

    describe "count_all!/2 - [in: keys])" do
      test "ok: returns the count of the requested keys", %{cache: cache} do
        assert cache.put_all(a: 1, c: 3, d: 4) == :ok

        # count = 2 keys * 3 levels = 6
        assert cache.count_all!(in: [:a, :b, :c]) == 6

        # total = 3 keys per level * 3 levels = 9
        assert cache.delete_all!() == 9
      end

      test "ok: returns 0 when none of the given keys is in cache", %{cache: cache} do
        assert cache.count_all!(in: ["foo", "bar", 1, :a]) == 0
      end

      test "ok: returns 0 when the given key list is empty", %{cache: cache} do
        assert cache.count_all!(in: []) == 0
      end

      test "error: raises an exception because invalid query spec", %{cache: cache} do
        assert_raise NimbleOptions.ValidationError, ~r"invalid value for :in option", fn ->
          cache.count_all!(in: :invalid)
        end
      end
    end

    describe "delete_all!/2 - [in: keys]" do
      test "ok: returns the count of the deleted keys", %{cache: cache} do
        assert cache.put_all(a: 1, c: 3, d: 4) == :ok

        # deleted = 2 keys * 3 levels = 6 (total: 3 keys * 3 levels = 9)
        assert cache.delete_all!(in: [:a, :b, :c]) == 6
        assert cache.get_all!() == [d: 4]

        assert cache.delete_all!() == 3
        assert cache.get_all!() == []
      end

      test "ok: returns 0 when none of the given keys is in cache", %{cache: cache} do
        assert cache.delete_all!(in: ["foo", "bar", 1, :a]) == 0
      end

      test "ok: returns 0 when the given key list is empty", %{cache: cache} do
        assert cache.delete_all!(in: []) == 0
      end

      test "error: raises an exception because invalid query spec", %{cache: cache} do
        assert_raise NimbleOptions.ValidationError, ~r"invalid value for :in option", fn ->
          cache.delete_all!(in: :invalid)
        end
      end
    end
  end
end
