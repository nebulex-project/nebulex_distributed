defmodule Nebulex.MultilevelInfoTest do
  import Nebulex.CacheCase

  deftests do
    alias Nebulex.Adapter
    alias Nebulex.Adapters.Common.Info.Stats

    describe "info/1" do
      @info_items Enum.sort([:server, :stats, :memory, :levels_info])
      @empty_stats Stats.new()

      test "ok: returns all info", %{cache: cache, name: name} do
        assert {:ok, info} = cache.info()
        assert Map.keys(info) |> Enum.sort() == @info_items

        assert info[:server] == server_info(name)
        assert info[:stats] == @empty_stats
        assert %{total: _, used: _} = info[:memory]

        expected_server_info = levels_server_info(name)

        info[:levels_info]
        |> Enum.with_index()
        |> Enum.each(fn {l, idx} ->
          assert l.server == Enum.at(expected_server_info, idx)
          assert l.stats == @empty_stats
          assert %{total: _, used: _} = l.memory
        end)
      end

      test "ok: returns item's info", %{cache: cache, name: name} do
        assert cache.info!(:server) == server_info(name)
        assert cache.info!(:stats) == @empty_stats
        assert %{total: _, used: _} = cache.info!(:memory)

        expected_server_info = levels_server_info(name)

        cache.info!(:levels_info)
        |> Enum.with_index()
        |> Enum.each(fn {l, idx} ->
          assert l.server == Enum.at(expected_server_info, idx)
          assert l.stats == @empty_stats
          assert %{total: _, used: _} = l.memory
        end)
      end

      test "ok: returns multiple items info", %{cache: cache, name: name} do
        assert cache.info!([]) == %{}

        assert cache.info!([:server]) == %{server: server_info(name)}
        assert %{memory: %{total: _, used: _}} = cache.info!([:memory])

        assert %{
                 server: server,
                 stats: stats,
                 levels_info: levels_info,
                 memory: %{total: _, used: _}
               } = info = cache.info!([:server, :stats, :levels_info, :memory])

        assert Map.keys(info) |> Enum.sort() == @info_items

        assert server == server_info(name)
        assert stats == @empty_stats

        expected_server_info = levels_server_info(name)

        levels_info
        |> Enum.with_index()
        |> Enum.each(fn {l, idx} ->
          assert l.server == Enum.at(expected_server_info, idx)
          assert l.stats == @empty_stats
          assert %{total: _, used: _} = l.memory
        end)

        assert %{server: server, memory: %{total: _, used: _}} =
                 info = cache.info!([:server, :memory])

        assert Map.keys(info) |> Enum.sort() == Enum.sort([:server, :memory])
        assert server == server_info(name)
      end

      test "ok: returns info for the given level", %{cache: cache, name: name} do
        assert cache.info!(:server, level: 1) == server_info(name)
        assert cache.info!(:stats, level: 2) == @empty_stats
        assert %{total: _, used: _} = cache.info!(:memory, level: 3)

        assert %{levels_info: [l]} = cache.info!([:levels_info, :server, :stats, :memory], level: 1)
        assert l.server == levels_server_info(name) |> Enum.at(0)
        assert l.stats == @empty_stats
        assert %{total: _, used: _} = l.memory
      end

      test "error: invalid info request", %{cache: cache} do
        for spec <- [:unknown, [:memory, :unknown], [:unknown, :unknown]] do
          assert_raise RuntimeError,
                       ~r/\*\* \(ArgumentError\) invalid information specification key :unknown/,
                       fn ->
                         cache.info!(spec)
                       end
        end
      end

      defp server_info(name) do
        adapter_meta = Adapter.lookup_meta(name)

        %{
          nbx_version: Nebulex.vsn(),
          cache_module: adapter_meta[:cache],
          cache_adapter: adapter_meta[:adapter],
          cache_name: adapter_meta[:name],
          cache_pid: adapter_meta[:pid]
        }
      end

      defp levels_server_info(name) do
        name
        |> Adapter.lookup_meta()
        |> Map.fetch!(:levels)
        |> Enum.map(fn %{name: name} ->
          adapter_meta = Adapter.lookup_meta(name)

          %{
            nbx_version: Nebulex.vsn(),
            cache_module: adapter_meta[:cache],
            cache_adapter: adapter_meta[:adapter],
            cache_name: adapter_meta[:name],
            cache_pid: adapter_meta[:pid]
          }
        end)
      end
    end
  end
end
