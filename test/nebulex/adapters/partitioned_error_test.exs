defmodule Nebulex.Adapters.PartitionedErrorTest do
  use ExUnit.Case, async: true
  use Mimic

  # Inherit tests
  use Nebulex.Cache.KVErrorTest
  use Nebulex.Cache.KVExpirationErrorTest
  use Nebulex.DistributedQueryableErrorTest

  import Nebulex.CacheCase, only: [setup_with_dynamic_cache: 2]

  defmodule ErrorCache do
    use Nebulex.Cache,
      otp_app: :nebulex_distributed,
      adapter: Nebulex.Adapters.Partitioned,
      adapter_opts: [primary_storage_adapter: Nebulex.FakeAdapter]
  end

  setup_with_dynamic_cache ErrorCache, __MODULE__
end
