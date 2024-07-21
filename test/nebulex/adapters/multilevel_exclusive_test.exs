defmodule Nebulex.Adapters.MultilevelExclusiveTest do
  use Nebulex.NodeCase

  use Nebulex.CacheTestCase,
    except: [
      Nebulex.Cache.QueryableTest,
      Nebulex.Cache.QueryableExpirationTest,
      Nebulex.Cache.QueryableQueryErrorTest
    ]

  use Nebulex.DistributedTest
  use Nebulex.MultilevelTest
  use Nebulex.MultilevelQueryableTest

  import Nebulex.CacheCase

  alias Nebulex.Cluster
  alias Nebulex.Distributed.TestCache.{Multilevel, MultilevelNil}
  alias Nebulex.Distributed.TestCache.Multilevel.{L1, L2, L3}

  @gc_interval :timer.hours(1)

  @levels [
    {
      L1,
      name: :multilevel_exclusive_l1, gc_interval: @gc_interval
    },
    {
      L2,
      name: :multilevel_exclusive_l2, gc_interval: @gc_interval
    },
    {
      L3,
      name: :multilevel_exclusive_l3, primary: [gc_interval: @gc_interval]
    }
  ]

  setup_with_dynamic_cache Multilevel, :multilevel_exclusive,
    inclusion_policy: :exclusive,
    levels: @levels

  setup do
    {:ok, pid} =
      MultilevelNil.start_link(
        name: :multilevel_exclusive_nil,
        inclusion_policy: :exclusive,
        levels: [
          {MultilevelNil.L1, name: :multilevel_exclusive_nil_l1, gc_interval: @gc_interval},
          {MultilevelNil.L2, name: :multilevel_exclusive_nil_l2}
        ]
      )

    default_dynamic_cache = MultilevelNil.get_dynamic_cache()
    _ = MultilevelNil.put_dynamic_cache(:multilevel_exclusive_nil)

    on_exit(fn ->
      try do
        safe_stop(pid)
      after
        MultilevelNil.put_dynamic_cache(default_dynamic_cache)
      end
    end)

    {:ok, nil_cache: MultilevelNil}
  end

  describe "multilevel exclusive" do
    test "policy" do
      assert Multilevel.inclusion_policy(:multilevel_exclusive) == :exclusive
      assert MultilevelNil.inclusion_policy(:multilevel_exclusive_nil) == :exclusive
    end

    test "get" do
      :ok = Multilevel.put(1, 1, level: 1)
      :ok = Multilevel.put(2, 2, level: 2)
      :ok = Multilevel.put(3, 3, level: 3)

      assert Multilevel.get!(1) == 1
      assert Multilevel.get!(2, return: :key) == 2
      assert Multilevel.get!(3) == 3
      refute Multilevel.get!(2, nil, level: 1)
      refute Multilevel.get!(3, nil, level: 1)
      refute Multilevel.get!(1, nil, level: 2)
      refute Multilevel.get!(3, nil, level: 2)
      refute Multilevel.get!(1, nil, level: 3)
      refute Multilevel.get!(2, nil, level: 3)
    end
  end

  describe "distributed levels" do
    test "return cluster nodes" do
      assert Cluster.get_nodes(:multilevel_exclusive_l3) == [node()]
    end

    test "joining new node" do
      node = :"node1@127.0.0.1"

      {:ok, pid} =
        start_cache(node, Multilevel,
          name: :multilevel_exclusive,
          inclusion_policy: :exclusive,
          levels: @levels
        )

      # check cluster nodes
      assert Cluster.get_nodes(:multilevel_exclusive_l3) == [node, node()]

      kv_pairs = for k <- 1..100, do: {k, k}

      Multilevel.transaction(fn ->
        assert Multilevel.put_all(kv_pairs) == :ok

        for k <- 1..100 do
          assert Multilevel.get!(k) == k
        end
      end)

      :ok = stop_cache(:"node1@127.0.0.1", pid)
    end
  end
end
