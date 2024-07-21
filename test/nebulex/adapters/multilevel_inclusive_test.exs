defmodule Nebulex.Adapters.MultilevelInclusiveTest do
  use Nebulex.NodeCase
  use Mimic

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
  import Nebulex.Utils, only: [wrap_error: 2]

  alias Nebulex.Cluster
  alias Nebulex.Distributed.TestCache.{Multilevel, MultilevelNil}
  alias Nebulex.Distributed.TestCache.Multilevel.{L1, L2, L3}

  @gc_interval :timer.hours(1)

  @levels [
    {
      L1,
      name: :multilevel_inclusive_l1, gc_interval: @gc_interval
    },
    {
      L2,
      name: :multilevel_inclusive_l2, gc_interval: @gc_interval
    },
    {
      L3,
      name: :multilevel_inclusive_l3, primary: [gc_interval: @gc_interval]
    }
  ]

  setup_with_dynamic_cache Multilevel, :multilevel_inclusive,
    inclusion_policy: :inclusive,
    levels: @levels

  setup do
    {:ok, pid} =
      MultilevelNil.start_link(
        levels: [
          {MultilevelNil.L1, gc_interval: @gc_interval},
          {MultilevelNil.L2, []}
        ]
      )

    on_exit(fn -> safe_stop(pid) end)

    {:ok, nil_cache: MultilevelNil}
  end

  describe "multilevel inclusive" do
    test "policy" do
      assert Multilevel.inclusion_policy(:multilevel_inclusive) == :inclusive
      assert MultilevelNil.inclusion_policy() == :inclusive
    end

    test "get" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      assert Multilevel.get!(1) == 1
      refute Multilevel.get!(1, nil, level: 2)
      refute Multilevel.get!(1, nil, level: 3)

      assert Multilevel.get!(2) == 2
      assert Multilevel.get!(2, nil, level: 1) == 2
      assert Multilevel.get!(2, nil, level: 2) == 2
      refute Multilevel.get!(2, nil, level: 3)

      assert Multilevel.get!(3, nil, level: 3) == 3
      refute Multilevel.get!(3, nil, level: 1)
      refute Multilevel.get!(3, nil, level: 2)

      assert Multilevel.get!(3) == 3
      assert Multilevel.get!(3, nil, level: 1) == 3
      assert Multilevel.get!(3, nil, level: 2) == 3
      assert Multilevel.get!(3, nil, level: 3) == 3
    end

    test "get command replicates data with [ttl: :infinity]" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      L1
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:error, %Nebulex.KeyError{key: 3}}
      end)

      L2
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:error, %Nebulex.KeyError{key: 3}}
      end)

      L3
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:ok, 3}
      end)
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:error, %Nebulex.KeyError{key: 3}}
      end)

      assert Multilevel.get!(3) == 3
    end

    test "ttl command fails when replicating data" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      L1
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:error, %Nebulex.KeyError{key: 3}}
      end)

      L2
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:error, %Nebulex.KeyError{key: 3}}
      end)

      L3
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:ok, 3}
      end)
      |> expect(:with_dynamic_cache, fn _, _ ->
        wrap_error Nebulex.Error, reason: :replication_failed
      end)

      assert_raise Nebulex.Error, ~r"command failed with reason: :replication_failed", fn ->
        Multilevel.get!(3)
      end
    end

    test "put command fails when replicating data" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      L1
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:error, %Nebulex.KeyError{key: 3}}
      end)

      L2
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:error, %Nebulex.KeyError{key: 3}}
      end)

      L3
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:ok, 3}
      end)
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:error, %Nebulex.KeyError{key: 3}}
      end)

      L1
      |> expect(:with_dynamic_cache, fn _, _ ->
        wrap_error Nebulex.Error, reason: :replication_failed
      end)

      assert_raise Nebulex.Error, ~r"command failed with reason: :replication_failed", fn ->
        Multilevel.get!(3)
      end
    end

    test "get_all level error" do
      L1
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:ok, [{1, 1}]}
      end)

      L2
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:ok, [{2, 2}]}
      end)

      L3
      |> expect(:with_dynamic_cache, fn _, _ ->
        wrap_error Nebulex.Error, reason: :error
      end)

      assert_raise Nebulex.Error, ~r"command failed with reason: :error", fn ->
        Multilevel.get_all!([in: [1, 2, 3]], replicate: false)
      end
    end

    test "get_all [replicate: true]" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      assert Multilevel.get_all!(in: [1]) |> Map.new() == %{1 => 1}
      refute Multilevel.get!(1, nil, level: 2)
      refute Multilevel.get!(1, nil, level: 3)

      assert Multilevel.get_all!(in: [1, 2]) |> Map.new() == %{1 => 1, 2 => 2}
      assert Multilevel.get!(2, nil, level: 1) == 2
      assert Multilevel.get!(2, nil, level: 2) == 2
      refute Multilevel.get!(2, nil, level: 3)

      assert Multilevel.get!(3, nil, level: 3) == 3
      refute Multilevel.get!(3, nil, level: 1)
      refute Multilevel.get!(3, nil, level: 2)

      assert Multilevel.get_all!(in: [1, 2, 3]) |> Map.new() == %{1 => 1, 2 => 2, 3 => 3}
      assert Multilevel.get!(3, nil, level: 1) == 3
      assert Multilevel.get!(3, nil, level: 2) == 3
      assert Multilevel.get!(3, nil, level: 3) == 3
    end

    test "get_all [replicate: false]" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      assert Multilevel.get_all!([in: [1]], replicate: false) |> Map.new() == %{1 => 1}
      refute Multilevel.get!(1, nil, level: 2)
      refute Multilevel.get!(1, nil, level: 3)

      assert Multilevel.get_all!([in: [1, 2]], replicate: false) |> Map.new() == %{1 => 1, 2 => 2}
      refute Multilevel.get!(2, nil, level: 1)
      assert Multilevel.get!(2, nil, level: 2) == 2
      refute Multilevel.get!(2, nil, level: 3)

      assert Multilevel.get!(3, nil, level: 3) == 3
      refute Multilevel.get!(3, nil, level: 1)
      refute Multilevel.get!(3, nil, level: 2)

      assert Multilevel.get_all!([in: [1, 2, 3]], replicate: false) |> Map.new() == %{
               1 => 1,
               2 => 2,
               3 => 3
             }

      refute Multilevel.get!(3, nil, level: 1)
      refute Multilevel.get!(3, nil, level: 2)
      assert Multilevel.get!(3, nil, level: 3) == 3
    end

    test "get boolean" do
      :ok = Multilevel.put(1, true, level: 1)
      :ok = Multilevel.put(2, false, level: 1)

      assert Multilevel.get!(1) == true
      assert Multilevel.get!(2) == false
    end

    test "fetched value is replicated with TTL on previous levels" do
      assert Multilevel.put(:a, 1, ttl: 1000) == :ok
      assert Multilevel.ttl!(:a) > 0

      _ = t_sleep(1100)

      refute Multilevel.get!(:a, nil, level: 1)
      refute Multilevel.get!(:a, nil, level: 2)
      refute Multilevel.get!(:a, nil, level: 3)

      assert Multilevel.put(:b, 1, level: 3) == :ok
      assert Multilevel.ttl!(:b) == :infinity
      assert Multilevel.expire!(:b, 1000)
      assert Multilevel.ttl!(:b) > 0
      refute Multilevel.get!(:b, nil, level: 1)
      refute Multilevel.get!(:b, nil, level: 2)
      assert Multilevel.get!(:b, nil, level: 3) == 1

      assert Multilevel.get!(:b) == 1
      assert Multilevel.get!(:b, nil, level: 1) == 1
      assert Multilevel.get!(:b, nil, level: 2) == 1
      assert Multilevel.get!(:b, nil, level: 3) == 1

      _ = t_sleep(1100)

      refute Multilevel.get!(:b, nil, level: 1)
      refute Multilevel.get!(:b, nil, level: 2)
      refute Multilevel.get!(:b, nil, level: 3)
    end
  end

  describe "distributed levels" do
    test "return cluster nodes" do
      assert Cluster.get_nodes(:multilevel_inclusive_l3) == [node()]
    end

    test "joining new node" do
      node = :"node1@127.0.0.1"

      {:ok, pid} =
        start_cache(node, Multilevel,
          name: :multilevel_inclusive,
          inclusion_policy: :inclusive,
          levels: @levels
        )

      # check cluster nodes
      assert Cluster.get_nodes(:multilevel_inclusive_l3) == [node, node()]

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
