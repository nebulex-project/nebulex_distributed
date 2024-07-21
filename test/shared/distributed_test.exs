defmodule Nebulex.DistributedTest do
  import Nebulex.CacheCase

  deftests do
    describe "queryable error:" do
      test "get_all/2 raises an exception because of an invalid query", %{cache: cache} do
        assert_query_error(fn ->
          cache.get_all(query: :invalid)
        end)
      end

      test "stream/2 raises an exception because of an invalid query", %{cache: cache} do
        assert_query_error(fn ->
          cache.stream(query: :invalid)
          |> elem(1)
          |> Enum.to_list()
        end)
      end

      test "stream/2 [on_error: :raise] raises an exception on command error", %{nil_cache: cache} do
        assert_raise Nebulex.Error, ~r/command failed with reason: :error/, fn ->
          cache.stream([], after_return: &cache.return_error/1)
          |> elem(1)
          |> Enum.to_list()
        end

        assert_raise Nebulex.Error, ~r/command failed with reason: :error/, fn ->
          cache.stream([in: [1, 2, 3]], after_return: &cache.return_error/1)
          |> elem(1)
          |> Enum.to_list()
        end
      end

      test "stream/2 [on_error: :nothing] skips command errors", %{nil_cache: cache} do
        assert cache.stream([], on_error: :nothing, after_return: &cache.return_error/1)
               |> elem(1)
               |> Enum.to_list() == []

        assert cache.stream([in: [1, 2, 3]],
                 on_error: :nothing,
                 after_return: &cache.return_error/1
               )
               |> elem(1)
               |> Enum.to_list() == []
      end

      test "stream/2 returns empty when is evaluated", %{nil_cache: cache} do
        assert cache.stream!(in: []) |> Enum.to_list() == []
      end

      test "delete_all/2 raises an exception because of an invalid query", %{cache: cache} do
        assert_query_error(fn ->
          cache.delete_all(query: :invalid)
        end)
      end

      test "count_all/2 raises an exception because of an invalid query", %{cache: cache} do
        assert_query_error(fn ->
          cache.count_all(query: :invalid)
        end)
      end

      defp assert_query_error(fun) do
        fun.()
      rescue
        e in [RuntimeError, Nebulex.QueryError] ->
          assert Regex.match?(~r/\*\* \(Nebulex.QueryError\) invalid query/, e.message) or
                   Regex.match?(~r/invalid query/, e.message)
      end
    end
  end
end
