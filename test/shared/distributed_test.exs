defmodule Nebulex.DistributedTest do
  import Nebulex.CacheCase

  deftests do
    alias Nebulex.Adapter
    alias Nebulex.Adapters.Common.Info.Stats

    describe "queryable error:" do
      test "get_all/2 raises an exception because of an invalid query", %{cache: cache} do
        assert_raise RuntimeError, ~r|\*\* \(Nebulex.QueryError\) invalid query|, fn ->
          cache.get_all(query: :invalid)
        end
      end

      test "stream/2 raises an exception because of an invalid query", %{cache: cache} do
        assert_raise RuntimeError, ~r|\*\* \(Nebulex.QueryError\) invalid query|, fn ->
          cache.stream(query: :invalid)
          |> elem(1)
          |> Enum.to_list()
        end
      end

      test "stream/2 [on_error: :raise] raises an exception on command error", %{nil_cache: cache} do
        assert_raise Nebulex.Error, ~r|command failed with reason: :error|, fn ->
          cache.stream([], after_return: &cache.return_error/1)
          |> elem(1)
          |> Enum.to_list()
        end

        assert_raise Nebulex.Error, ~r|command failed with reason: :error|, fn ->
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

      test "delete_all/2 raises an exception because of an invalid query", %{cache: cache} do
        assert_raise RuntimeError, ~r|\*\* \(Nebulex.QueryError\) invalid query|, fn ->
          cache.delete_all(query: :invalid)
        end
      end

      test "count_all/2 raises an exception because of an invalid query", %{cache: cache} do
        assert_raise RuntimeError, ~r|\*\* \(Nebulex.QueryError\) invalid query|, fn ->
          cache.count_all(query: :invalid)
        end
      end
    end

    describe "info/1" do
      @empty_stats Stats.new()

      test "ok: returns all info", %{cache: cache, name: name} do
        assert {:ok, info} = cache.info()

        assert info[:server] == server_info(name)
        assert info[:nodes] |> Enum.sort() == cache.nodes(name) |> Enum.sort()
        assert info[:stats] == @empty_stats
        assert %{total: _, used: _} = info[:memory]
      end

      test "ok: returns item's info", %{cache: cache, name: name} do
        assert cache.info!(:server) == server_info(name)
        assert cache.info!(:nodes) |> Enum.sort() == cache.nodes(name) |> Enum.sort()
        assert cache.info!(:stats) == @empty_stats
        assert %{total: _, used: _} = cache.info!(:memory)
      end

      test "ok: returns multiple items info", %{cache: cache, name: name} do
        assert cache.info!([:server]) == %{server: server_info(name)}
        assert %{memory: %{total: _, used: _}} = cache.info!([:memory])

        assert %{server: server, nodes: nodes, memory: %{total: _, used: _}} =
                 cache.info!([:server, :nodes, :memory])

        assert server == server_info(name)
        assert Enum.sort(nodes) == cache.nodes(name) |> Enum.sort()

        assert info = cache.info!([:server, :memory])

        assert Map.keys(info) |> Enum.sort() == Enum.sort([:server, :memory])
        assert info.server == server_info(name)
      end

      test "error: invalid info request", %{cache: cache} do
        for spec <- [:unknown, [:memory, :unknown], [:unknown, :unknown]] do
          assert_raise RuntimeError,
                       ~r|\*\* \(ArgumentError\) invalid information specification key :unknown|,
                       fn ->
                         cache.info!(spec)
                       end
        end
      end

      defp server_info(name) do
        {:ok, adapter_meta} = Adapter.lookup_meta(name)

        %{
          nbx_version: Nebulex.vsn(),
          cache_module: adapter_meta[:cache],
          cache_adapter: adapter_meta[:adapter],
          cache_name: adapter_meta[:name],
          cache_pid: adapter_meta[:pid]
        }
      end
    end
  end
end
