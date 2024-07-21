defmodule Nebulex.DistributedInfoTest do
  import Nebulex.CacheCase

  deftests do
    alias Nebulex.Adapter
    alias Nebulex.Adapters.Common.Info.Stats

    describe "info/1" do
      @info_items Enum.sort([:server, :stats, :memory, :nodes, :nodes_info])
      @empty_stats Stats.new()

      test "ok: returns all info", %{cache: cache, name: name} do
        nodes = cache.nodes(name) |> Enum.sort()

        assert {:ok, info} = cache.info()
        assert Map.keys(info) |> Enum.sort() == @info_items

        assert info[:server] == server_info(name)
        assert info[:nodes] |> Enum.sort() == nodes
        assert info[:stats] == @empty_stats
        assert %{total: _, used: _} = info[:memory]

        for {node, node_info} <- info[:nodes_info] do
          assert node in nodes
          assert node_info[:server] |> Map.delete(:cache_pid) == primary_server_info(name)
          assert node_info[:stats] == @empty_stats
          assert %{total: _, used: _} = node_info[:memory]
        end
      end

      test "ok: returns item's info", %{cache: cache, name: name} do
        nodes = cache.nodes(name) |> Enum.sort()

        assert cache.info!(:server) == server_info(name)
        assert cache.info!(:nodes) |> Enum.sort() == nodes
        assert cache.info!(:stats) == @empty_stats
        assert %{total: _, used: _} = cache.info!(:memory)

        assert nodes_info = cache.info!(:nodes_info)

        for {node, node_info} <- nodes_info do
          assert node in nodes
          assert node_info[:server] |> Map.delete(:cache_pid) == primary_server_info(name)
          assert node_info[:stats] == @empty_stats
          assert %{total: _, used: _} = node_info[:memory]
        end
      end

      test "ok: returns multiple items info", %{cache: cache, name: name} do
        assert cache.info!([]) == %{}

        assert cache.info!([:server]) == %{server: server_info(name)}
        assert %{memory: %{total: _, used: _}} = cache.info!([:memory])

        assert %{
                 server: server,
                 stats: stats,
                 nodes: nodes,
                 nodes_info: nodes_info,
                 memory: %{total: _, used: _}
               } = info = cache.info!([:server, :stats, :nodes, :nodes_info, :memory])

        assert Map.keys(info) |> Enum.sort() == @info_items
        assert server == server_info(name)
        assert stats == @empty_stats
        assert Enum.sort(nodes) == cache.nodes(name) |> Enum.sort()

        for {node, node_info} <- nodes_info do
          assert node in nodes
          assert node_info[:server] |> Map.delete(:cache_pid) == primary_server_info(name)
          assert node_info[:stats] == @empty_stats
          assert %{total: _, used: _} = node_info[:memory]
        end

        assert %{server: server, memory: %{total: _, used: _}} =
                 info = cache.info!([:server, :memory])

        assert Map.keys(info) |> Enum.sort() == Enum.sort([:server, :memory])
        assert server == server_info(name)
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

      defp primary_server_info(name) do
        adapter_meta =
          name
          |> Adapter.lookup_meta()
          |> Map.fetch!(:primary_name)
          |> Adapter.lookup_meta()

        %{
          nbx_version: Nebulex.vsn(),
          cache_module: adapter_meta[:cache],
          cache_adapter: adapter_meta[:adapter],
          cache_name: adapter_meta[:name]
        }
      end
    end
  end
end
