defmodule Nebulex.DistributedCommonTest do
  @moduledoc """
  Common tests.
  """

  defmacro __using__(_opts) do
    quote do
      use Nebulex.Cache.KVTest
      use Nebulex.Cache.KVExpirationTest
      use Nebulex.Cache.KVPropTest
      use Nebulex.Cache.QueryableTest
      use Nebulex.Cache.QueryableExpirationTest
      use Nebulex.Cache.TransactionTest
    end
  end
end
