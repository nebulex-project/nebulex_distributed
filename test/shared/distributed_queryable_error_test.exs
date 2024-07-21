defmodule Nebulex.DistributedQueryableErrorTest do
  import Nebulex.CacheCase

  deftests do
    describe "get_all/2" do
      test "error: command failed", %{cache: cache} do
        assert {:error,
                %Nebulex.Error{
                  module: Nebulex.Adapters.Partitioned,
                  reason: {:rpc, {:unexpected_errors, _}}
                }} = cache.get_all()
      end

      test "error: raises an exception", %{cache: cache} do
        assert_raise Nebulex.Error, fn ->
          cache.get_all!()
        end
      end

      test "error: get keys failed", %{cache: cache} do
        assert cache.get_all(in: [1, 2, 3]) ==
                 {:error,
                  %Nebulex.Error{
                    module: Nebulex.Error,
                    reason: :error,
                    metadata: []
                  }}
      end
    end

    describe "count_all/2" do
      test "error: command failed", %{cache: cache} do
        assert {:error,
                %Nebulex.Error{
                  module: Nebulex.Adapters.Partitioned,
                  reason: {:rpc, {:unexpected_errors, _}}
                }} = cache.count_all()
      end

      test "error: raises an exception", %{cache: cache} do
        assert_raise Nebulex.Error, fn ->
          cache.count_all!()
        end
      end

      test "error: count keys failed", %{cache: cache} do
        assert cache.count_all(in: [1, 2, 3]) ==
                 {:error,
                  %Nebulex.Error{
                    module: Nebulex.Error,
                    reason: :error,
                    metadata: []
                  }}
      end
    end

    describe "delete_all/2" do
      test "error: command failed", %{cache: cache} do
        assert {:error,
                %Nebulex.Error{
                  module: Nebulex.Adapters.Partitioned,
                  reason: {:rpc, {:unexpected_errors, _}}
                }} = cache.delete_all()
      end

      test "error: raises an exception", %{cache: cache} do
        assert_raise Nebulex.Error, fn ->
          cache.delete_all!()
        end
      end

      test "error: delete keys failed", %{cache: cache} do
        assert cache.delete_all(in: [1, 2, 3]) ==
                 {:error,
                  %Nebulex.Error{
                    module: Nebulex.Error,
                    reason: :error,
                    metadata: []
                  }}
      end
    end

    describe "stream/2 command failure" do
      test "raises an exception", %{cache: cache} do
        assert_raise Nebulex.Error, fn ->
          cache.stream!()
          |> Enum.to_list()
        end
      end

      test "raises an exception (in: [...])", %{cache: cache} do
        assert_raise Nebulex.Error, fn ->
          cache.stream!(in: [1, 2, 3])
          |> Enum.to_list()
        end
      end
    end
  end
end
