defmodule Nebulex.Adapters.Partitioned do
  @moduledoc """
  Built-in adapter for partitioned cache topology.

  ## Overall features

    * Partitioned cache topology (Sharding Distribution Model).
    * Configurable primary storage adapter.
    * Configurable Keyslot to distributed the keys across the cluster members.
    * Support for transactions via Erlang global name registration facility.
    * Stats support rely on the primary storage adapter.

  ## Partitioned Cache Topology

  There are several key points to consider about a partitioned cache:

    * _**Partitioned**_: The data in a distributed cache is spread out over
      all the servers in such a way that no two servers are responsible for
      the same piece of cached data. This means that the size of the cache
      and the processing power associated with the management of the cache
      can grow linearly with the size of the cluster. Also, it means that
      operations against data in the cache can be accomplished with a
      "single hop," in other words, involving at most one other server.

    * _**Load-Balanced**_:  Since the data is spread out evenly over the
      servers, the responsibility for managing the data is automatically
      load-balanced across the cluster.

    * _**Ownership**_: Exactly one node in the cluster is responsible for each
      piece of data in the cache.

    * _**Point-To-Point**_: The communication for the partitioned cache is all
      point-to-point, enabling linear scalability.

    * _**Location Transparency**_: Although the data is spread out across
      cluster nodes, the exact same API is used to access the data, and the
      same behavior is provided by each of the API methods. This is called
      location transparency, which means that the developer does not have to
      code based on the topology of the cache, since the API and its behavior
      will be the same with a local cache, a replicated cache, or a distributed
      cache.

    * _**Failover**_: Failover of a distributed cache involves promoting backup
      data to be primary storage. When a cluster node fails, all remaining
      cluster nodes determine what data each holds in backup that the failed
      cluster node had primary responsible for when it died. Those data becomes
      the responsibility of whatever cluster node was the backup for the data.
      However, this adapter does not provide fault-tolerance implementation,
      each piece of data is kept in a single node/machine (via sharding), then,
      if a node fails, the data kept by this node won't be available for the
      rest of the cluster members.

  > Based on **"Distributed Caching Essential Lessons"** by **Cameron Purdy**
    and [Coherence Partitioned Cache Service][oracle-pcs].

  [oracle-pcs]: https://docs.oracle.com/cd/E13924_01/coh.340/e13819/partitionedcacheservice.htm

  ## Additional implementation notes

  `:pg2` or `:pg` (>= OTP 23) is used under-the-hood by the adapter to manage
  the cluster nodes. When the partitioned cache is started in a node, it creates
  a group and joins it (the cache supervisor PID is joined to the group). Then,
  when a function is invoked, the adapter picks a node from the group members,
  and then the function is executed on that specific node. In the same way,
  when a partitioned cache supervisor dies (the cache is stopped or killed for
  some reason), the PID of that process is automatically removed from the PG
  group; this is why it's recommended to use consistent hashing for distributing
  the keys across the cluster nodes.

  > **NOTE:** `pg2` will be replaced by `pg` in future, since the `pg2` module
    is deprecated as of OTP 23 and scheduled for removal in OTP 24.

  This adapter depends on a local cache adapter (primary storage), it adds
  a thin layer on top of it in order to distribute requests across a group
  of nodes, where is supposed the local cache is running already. However,
  you don't need to define any additional cache module for the primary
  storage, instead, the adapter initializes it automatically (it adds the
  primary storage as part of the supervision tree) based on the given
  options within the `primary_storage_adapter:` argument.

  ## Usage

  When used, the Cache expects the `:otp_app` and `:adapter` as options.
  The `:otp_app` should point to an OTP application that has the cache
  configuration. For example:

      defmodule MyApp.PartitionedCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Partitioned
      end

  Optionally, you can configure the desired primary storage adapter with the
  option `:primary_storage_adapter`; defaults to `Nebulex.Adapters.Local`.

      defmodule MyApp.PartitionedCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Partitioned,
          primary_storage_adapter: Nebulex.Adapters.Local
      end

  Also, you can provide a custom keyslot function:

      defmodule MyApp.PartitionedCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Partitioned,
          primary_storage_adapter: Nebulex.Adapters.Local

        @behaviour Nebulex.Adapter.Keyslot

        @impl true
        def hash_slot(key, range) do
          key
          |> :erlang.phash2()
          |> :jchash.compute(range)
        end
      end

  Where the configuration for the cache must be in your application environment,
  usually defined in your `config/config.exs`:

      config :my_app, MyApp.PartitionedCache,
        keyslot: MyApp.PartitionedCache,
        primary: [
          gc_interval: 3_600_000,
          backend: :shards
        ]

  If your application was generated with a supervisor (by passing `--sup`
  to `mix new`) you will have a `lib/my_app/application.ex` file containing
  the application start callback that defines and starts your supervisor.
  You just need to edit the `start/2` function to start the cache as a
  supervisor on your application's supervisor:

      def start(_type, _args) do
        children = [
          {MyApp.PartitionedCache, []},
          ...
        ]

  See `Nebulex.Cache` for more information.

  ## Options

  This adapter supports the following options and all of them can be given via
  the cache configuration:

    * `:primary` - The options that will be passed to the adapter associated
      with the local primary storage. These options will depend on the local
      adapter to use.

    * `:keyslot` - Defines the module implementing `Nebulex.Adapter.Keyslot`
      behaviour.

    * `:join_timeout` - Interval time in milliseconds for joining the
      running partitioned cache to the cluster. This is to ensure it is
      always joined. Defaults to `:timer.seconds(180)`.

  ## Shared runtime options

  When using the partitioned adapter, all of the cache functions outlined in
  `Nebulex.Cache` accept the following options:

  #{Nebulex.Adapters.Partitioned.Options.common_runtime_options_docs()}

  ### Stream options

  The `stream` command supports the following options:

  #{Nebulex.Adapters.Partitioned.Options.stream_options_docs()}

  ## Telemetry events

  This adapter emits all recommended Telemetry events, and documented
  in `Nebulex.Cache` module (see **"Adapter-specific events"** section).

  Since the partitioned adapter depends on the configured primary storage
  adapter (local cache adapter), this one may also emit Telemetry events.
  Therefore, there will be events emitted by the partitioned adapter as well
  as the primary storage adapter. For example, for the cache defined before
  `MyApp.PartitionedCache`, these would be the emitted events:

    * `[:my_app, :partitioned_cache, :command, :start]`
    * `[:my_app, :partitioned_cache, :primary, :command, :start]`
    * `[:my_app, :partitioned_cache, :command, :stop]`
    * `[:my_app, :partitioned_cache, :primary, :command, :stop]`
    * `[:my_app, :partitioned_cache, :command, :exception]`
    * `[:my_app, :partitioned_cache, :primary, :command, :exception]`

  As you may notice, the telemetry prefix by default for the partitioned cache
  is `[:my_app, :partitioned_cache]`, and the prefix for its primary storage
  `[:my_app, :partitioned_cache, :primary]`.

  See also the [Telemetry guide](http://hexdocs.pm/nebulex/telemetry.html)
  for more information and examples.

  ## Adapter-specific telemetry events

  This adapter exposes following Telemetry events:

    * `telemetry_prefix ++ [:bootstrap, :started]` - Dispatched by the adapter
      when the bootstrap process is started.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          cluster_nodes: [node]
        }
        ```

    * `telemetry_prefix ++ [:bootstrap, :stopped]` - Dispatched by the adapter
      when the bootstrap process is stopped.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          cluster_nodes: [node],
          reason: term
        }
        ```

    * `telemetry_prefix ++ [:bootstrap, :exit]` - Dispatched by the adapter
      when the bootstrap has received an exit signal.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          cluster_nodes: [node],
          reason: term
        }
        ```

    * `telemetry_prefix ++ [:bootstrap, :joined]` - Dispatched by the adapter
      when the bootstrap has joined the cache to the cluster.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          cluster_nodes: [node]
        }
        ```

  ## Stats

  This adapter depends on the primary storage adapter for the stats support.
  Therefore, it is important to ensure the underlying primary storage adapter
  does support stats, otherwise, you may get unexpected errors.

  ## Extended API

  This adapter provides some additional convenience functions to the
  `Nebulex.Cache` API.

  Retrieving the primary storage or local cache module:

      MyCache.__primary__()

  Retrieving the cluster nodes associated with the given cache `name`:

      MyCache.nodes()

  Get a cluster node based on the given `key`:

      MyCache.get_node("mykey")

  Joining the cache to the cluster:

      MyCache.join_cluster()

  Leaving the cluster (removes the cache from the cluster):

      MyCache.leave_cluster()

  ## Caveats of partitioned adapter

  For `c:Nebulex.Cache.get_and_update/3` and `c:Nebulex.Cache.update/4`,
  they both have a parameter that is the anonymous function, and it is compiled
  into the module where it is created, which means it necessarily doesn't exists
  on remote nodes. To ensure they work as expected, you must provide functions
  from modules existing in all nodes of the group.
  """

  # Provide Cache Implementation
  @behaviour Nebulex.Adapter
  @behaviour Nebulex.Adapter.KV
  @behaviour Nebulex.Adapter.Queryable

  # Inherit default transaction implementation
  use Nebulex.Adapter.Transaction

  # Inherit default info implementation
  use Nebulex.Adapters.Common.Info

  # Inherit default keyslot implementation
  use Nebulex.Adapter.Keyslot

  import Nebulex.Adapter
  import Nebulex.Utils

  alias __MODULE__.Options
  alias Nebulex.{Cluster, RPC}

  ## Nebulex.Adapter

  @impl true
  defmacro __before_compile__(env) do
    otp_app = Module.get_attribute(env.module, :otp_app)
    opts = Module.get_attribute(env.module, :opts)
    adapter_opts = Keyword.fetch!(opts, :adapter_opts)

    adapter_opts = Options.validate_compile_opts!(adapter_opts)
    primary = Keyword.fetch!(adapter_opts, :primary_storage_adapter)

    quote do
      defmodule Primary do
        @moduledoc """
        This is the cache for the primary storage.
        """
        use Nebulex.Cache,
          otp_app: unquote(otp_app),
          adapter: unquote(primary)
      end

      @doc """
      A convenience function for getting the primary storage cache.
      """
      def __primary__, do: Primary

      @doc """
      A convenience function for getting the cluster nodes.
      """
      def nodes(name \\ get_dynamic_cache()) do
        Cluster.get_nodes(name)
      end

      @doc """
      A convenience function to get the node of the given `key`.
      """
      def get_node(name \\ get_dynamic_cache(), key) do
        with_meta(name, fn %{name: ^name, keyslot: keyslot} ->
          Cluster.get_node(name, key, keyslot)
        end)
      end

      @doc """
      A convenience function for joining the cache to the cluster.
      """
      def join_cluster(name \\ get_dynamic_cache()) do
        Cluster.join(name)
      end

      @doc """
      A convenience function for removing the cache from the cluster.
      """
      def leave_cluster(name \\ get_dynamic_cache()) do
        Cluster.leave(name)
      end
    end
  end

  @impl true
  def init(opts) do
    # Validate options
    opts = Options.validate_start_opts!(opts)

    # Required options
    telemetry_prefix = Keyword.fetch!(opts, :telemetry_prefix)
    telemetry = Keyword.fetch!(opts, :telemetry)
    cache = Keyword.fetch!(opts, :cache)
    name = opts[:name] || cache

    # Maybe use stats
    stats = Keyword.fetch!(opts, :stats)

    # Primary cache options
    primary_opts =
      Keyword.merge(
        [telemetry_prefix: telemetry_prefix ++ [:primary], telemetry: telemetry, stats: stats],
        Keyword.fetch!(opts, :primary)
      )

    # Maybe put a name to primary storage
    primary_opts =
      if opts[:name],
        do: [name: camelize_and_concat([name, Primary])] ++ primary_opts,
        else: primary_opts

    # Keyslot module for selecting nodes
    keyslot = Keyword.fetch!(opts, :keyslot)

    # Prepare metadata
    adapter_meta = %{
      telemetry_prefix: telemetry_prefix,
      telemetry: telemetry,
      name: name,
      primary_name: primary_opts[:name],
      keyslot: keyslot,
      stats: stats
    }

    # Prepare child spec
    child_spec =
      Supervisor.child_spec(
        {Nebulex.Adapters.Partitioned.Supervisor, {cache, name, adapter_meta, primary_opts, opts}},
        id: name
      )

    {:ok, child_spec, adapter_meta}
  end

  ## Nebulex.Adapter.KV

  @impl true
  def fetch(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :fetch, [key, opts], opts)
  end

  @impl true
  def put(adapter_meta, key, value, ttl, on_write, opts) do
    opts = Options.validate_common_runtime_opts!(opts)
    primary_opts = [ttl: ttl] ++ opts

    case on_write do
      :put ->
        with :ok <- call(adapter_meta, key, :put, [key, value, primary_opts], opts) do
          {:ok, true}
        end

      :put_new ->
        call(adapter_meta, key, :put_new, [key, value, primary_opts], opts)

      :replace ->
        call(adapter_meta, key, :replace, [key, value, primary_opts], opts)
    end
  end

  @impl true
  def put_all(adapter_meta, entries, ttl, on_write, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    case on_write do
      :put ->
        do_put_all(:put_all, adapter_meta, entries, ttl, opts)

      :put_new ->
        do_put_all(:put_new_all, adapter_meta, entries, ttl, opts)
    end
  end

  def do_put_all(action, adapter_meta, entries, ttl, opts) do
    timeout = Keyword.fetch!(opts, :timeout)
    opts = [ttl: ttl] ++ opts

    reducer = fn
      {:ok, :ok}, _, {_bool, acc} ->
        {:cont, {true, acc}}

      {:ok, {:ok, true}}, {_, {_, _, [_, _, [kv, _]]}}, {bool, acc} ->
        {:cont, {bool, Enum.reduce(kv, acc, &[elem(&1, 0) | &2])}}

      {:ok, {:ok, false}}, _, {_bool, acc} ->
        {:cont, {false, acc}}

      {:ok, {:error, _} = error}, _, {_bool, acc} ->
        {:halt, {error, acc}}

      {:error, _} = error, _, {_bool, acc} ->
        {:halt, {error, acc}}
    end

    case map_reduce(entries, adapter_meta, action, [opts], timeout, {true, []}, reducer) do
      {true, _} ->
        {:ok, true}

      {false, keys} ->
        :ok = Enum.each(keys, &delete(adapter_meta, &1, []))

        {:ok, false}

      {error, keys} ->
        :ok = Enum.each(keys, &delete(adapter_meta, &1, []))

        error
    end
  end

  @impl true
  def delete(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :delete, [key, opts], opts)
  end

  @impl true
  def take(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :take, [key, opts], opts)
  end

  @impl true
  def has_key?(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :has_key?, [key, opts], opts)
  end

  @impl true
  def update_counter(adapter_meta, key, amount, ttl, default, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :incr, [key, amount, [ttl: ttl, default: default] ++ opts], opts)
  end

  @impl true
  def ttl(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :ttl, [key, opts], opts)
  end

  @impl true
  def expire(adapter_meta, key, ttl, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :expire, [key, ttl, opts], opts)
  end

  @impl true
  def touch(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :touch, [key, opts], opts)
  end

  ## Nebulex.Adapter.Queryable

  @impl true
  def execute(adapter_meta, query, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    do_execute(adapter_meta, query, opts)
  end

  defp do_execute(_adapter_meta, %{op: :get_all, query: {:in, []}}, _opts) do
    {:ok, []}
  end

  defp do_execute(_adapter_meta, %{op: op, query: {:in, []}}, _opts)
       when op in [:count_all, :delete_all] do
    {:ok, 0}
  end

  defp do_execute(adapter_meta, %{op: op, query: {:in, keys}} = query, opts) do
    timeout = Keyword.fetch!(opts, :timeout)

    init_acc = if op == :get_all, do: [], else: 0

    query = build_query(query)
    group_fun = &Keyword.put(query, :in, &1)

    reducer = fn
      {:ok, {:ok, res}}, _, acc when is_list(res) ->
        {:cont, res ++ acc}

      {:ok, {:ok, res}}, _, acc when is_integer(res) ->
        {:cont, acc + res}

      {:ok, {:error, _} = error}, _, _ ->
        {:halt, error}

      {:error, _} = error, _, _ ->
        {:halt, error}
    end

    case map_reduce(keys, adapter_meta, op, [opts], timeout, init_acc, reducer, group_fun) do
      {:error, _} = error ->
        error

      other ->
        {:ok, other}
    end
  end

  defp do_execute(adapter_meta, %{op: op} = query, opts) do
    timeout = Keyword.fetch!(opts, :timeout)
    query = build_query(query)

    RPC.multicall(
      Cluster.get_nodes(adapter_meta.name),
      __MODULE__,
      :with_dynamic_cache,
      [adapter_meta, op, [query, opts]],
      timeout
    )
    |> handle_rpc_multi_call(op, reducer(op))
  end

  @impl true
  def stream(adapter_meta, query_meta, opts) do
    opts = Options.validate_stream_opts!(opts)

    do_stream(adapter_meta, query_meta, opts)
  end

  defp do_stream(_adapter_meta, %{query: {:in, []}}, _opts) do
    {:ok, Stream.map([], & &1)}
  end

  defp do_stream(adapter_meta, %{query: {:in, keys}} = query, opts) do
    timeout = Keyword.fetch!(opts, :timeout)
    {on_error, opts} = Keyword.pop!(opts, :on_error)

    query = build_query(query)

    reducer = fn
      {:ok, {:ok, res}}, _, acc ->
        {:cont, res ++ acc}

      {:ok, {:error, _} = error}, _, _ ->
        {:halt, error}

      {:error, _} = error, _, _ ->
        {:halt, error}
    end

    # TODO: Perhaps find a better way to create the stream
    Stream.resource(
      fn ->
        keys
        |> group_keys_by_node(adapter_meta, :stream)
        |> Enum.map(fn {node, group} ->
          {node, {__MODULE__, :eval_stream, [adapter_meta, Keyword.put(query, :in, group), opts]}}
        end)
      end,
      fn
        [] ->
          {:halt, []}

        groups ->
          case RPC.multi_mfa_call(groups, timeout, [], reducer) do
            {:error, _} = error when on_error == :nothing ->
              {:halt, error}

            {:error, reason} when on_error == :raise ->
              stacktrace =
                Process.info(self(), :current_stacktrace)
                |> elem(1)
                |> tl()

              reraise reason, stacktrace

            elements ->
              {[elements], []}
          end
      end,
      & &1
    )
    |> wrap_ok()
  end

  defp do_stream(adapter_meta, query, opts) do
    timeout = Keyword.fetch!(opts, :timeout)
    {on_error, opts} = Keyword.pop!(opts, :on_error)

    query = build_query(query)

    Stream.resource(
      fn ->
        Cluster.get_nodes(adapter_meta.name)
      end,
      fn
        [] ->
          {:halt, []}

        nodes ->
          RPC.multicall(
            nodes,
            __MODULE__,
            :eval_stream,
            [adapter_meta, query, opts],
            timeout
          )
          |> handle_rpc_multi_call(:stream, reducer(:stream))
          |> case do
            {:ok, elements} ->
              {[elements], []}

            {:error, _} = error when on_error == :nothing ->
              {:halt, error}

            {:error, reason} when on_error == :raise ->
              stacktrace =
                Process.info(self(), :current_stacktrace)
                |> elem(1)
                |> tl()

              reraise reason, stacktrace
          end
      end,
      & &1
    )
    |> wrap_ok()
  end

  ## Nebulex.Adapter.Transaction

  @impl true
  def transaction(adapter_meta, fun, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    super(adapter_meta, fun, Keyword.put(opts, :nodes, Cluster.get_nodes(adapter_meta.name)))
  end

  @impl true
  def in_transaction?(adapter_meta, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    super(adapter_meta, opts)
  end

  ## Nebulex.Adapter.Info

  @impl true
  def info(adapter_meta, spec, opts)

  def info(adapter_meta, :all, opts) do
    with {:ok, server} <- super(adapter_meta, :server, opts),
         {:ok, nodes} <- info(adapter_meta, :nodes, opts),
         {:ok, info} <- do_info(adapter_meta, :all, opts) do
      {:ok, Map.merge(info, %{server: server, nodes: nodes})}
    end
  end

  def info(adapter_meta, :server, opts) do
    super(adapter_meta, :server, opts)
  end

  def info(adapter_meta, :nodes, _opts) do
    {:ok, Cluster.get_nodes(adapter_meta.name)}
  end

  def info(adapter_meta, spec, opts) when is_list(spec) do
    {spec1, spec2} = Enum.split_with(spec, &(&1 in [:server, :nodes]))

    acc =
      Map.new(spec1, fn i ->
        {:ok, value} = info(adapter_meta, i, opts)

        {i, value}
      end)

    with {:ok, info} <- do_info(adapter_meta, spec2, opts) do
      {:ok, Map.merge(acc, info)}
    end
  end

  def info(adapter_meta, spec, opts) do
    do_info(adapter_meta, spec, opts)
  end

  defp do_info(adapter_meta, spec, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    with {:ok, results} <-
           RPC.multicall(
             Cluster.get_nodes(adapter_meta.name),
             __MODULE__,
             :with_dynamic_cache,
             [adapter_meta, :info, [spec, opts]],
             Keyword.fetch!(opts, :timeout)
           )
           |> handle_rpc_multi_call(:info, reducer(:info)) do
      results
      |> Map.values()
      |> Enum.reduce(%{}, fn node_info, acc ->
        node_info
        |> Map.delete(:server)
        |> merge_info_map(acc)
      end)
      |> wrap_ok()
    end
  end

  def merge_info_map(map1, map2) when is_map(map1) and is_map(map2) do
    Enum.reduce(map1, map2, fn
      {k, v}, acc when is_integer(v) ->
        Map.update(acc, k, v, &Kernel.+(&1, v))

      {k, v}, acc when is_map(v) ->
        Map.update(acc, k, v, &merge_info_map(&1, v))

      {k, v}, acc ->
        Map.put_new(acc, k, v)
    end)
  end

  ## Helpers

  @doc """
  Helper function to use dynamic cache for internal primary cache storage
  when needed.
  """
  def with_dynamic_cache(adapter_meta, action, args)

  def with_dynamic_cache(%{cache: cache, primary_name: nil}, action, args) do
    apply(cache.__primary__, action, args)
  end

  def with_dynamic_cache(%{cache: cache, primary_name: primary_name}, action, args) do
    cache.__primary__.with_dynamic_cache(primary_name, fn ->
      apply(cache.__primary__, action, args)
    end)
  end

  @doc """
  Helper to perform `stream/3` locally.
  """
  def eval_stream(meta, query, opts) do
    with {:ok, stream} <- with_dynamic_cache(meta, :stream, [query, opts]) do
      {:ok, Enum.to_list(stream)}
    end
  end

  ## Private Functions

  defp build_query(%{select: select, query: query}) do
    query = with {:q, q} <- query, do: {:query, q}

    [query, select: select]
  end

  defp reducer(:info) do
    &Map.new/1
  end

  defp reducer(op) when op in [:get_all, :stream] do
    fn result ->
      result
      |> Enum.map(&elem(&1, 1))
      |> List.flatten()
    end
  end

  defp reducer(op) when op in [:count_all, :delete_all] do
    fn result ->
      result
      |> Enum.map(&elem(&1, 1))
      |> Enum.sum()
    end
  end

  defp get_node(%{name: name, keyslot: keyslot}, key) do
    Cluster.get_node(name, key, keyslot)
  end

  defp call(adapter_meta, key, action, args, opts) do
    timeout = Keyword.fetch!(opts, :timeout)

    adapter_meta
    |> get_node(key)
    |> rpc_call(adapter_meta, action, args, timeout)
  end

  defp rpc_call(node, meta, fun, args, timeout) when is_map(meta) do
    RPC.call(node, __MODULE__, :with_dynamic_cache, [meta, fun, args], timeout)
  end

  # defp rpc_call(node, mod, fun, args, timeout) when is_atom(mod) do
  #   RPC.call(node, mod, fun, args, timeout)
  # end

  defp map_reduce(enum, meta, action, args, timeout, acc, reducer, group_fun \\ & &1) do
    enum
    |> group_keys_by_node(meta, action)
    |> Enum.map(fn {node, group} ->
      {node, {__MODULE__, :with_dynamic_cache, [meta, action, [group_fun.(group) | args]]}}
    end)
    |> RPC.multi_mfa_call(timeout, acc, reducer)
  end

  defp group_keys_by_node(enum, adapter_meta, action) when action in [:put_all, :put_new_all] do
    Enum.reduce(enum, %{}, fn {key, _} = entry, acc ->
      node = get_node(adapter_meta, key)

      Map.update(acc, node, [entry], &[entry | &1])
    end)
  end

  defp group_keys_by_node(enum, adapter_meta, action)
       when action in [:get_all, :count_all, :delete_all, :stream] do
    Enum.reduce(enum, %{}, fn key, acc ->
      node = get_node(adapter_meta, key)

      Map.update(acc, node, [key], &[key | &1])
    end)
  end

  defp handle_rpc_multi_call({res, []}, _action, fun) do
    {:ok, fun.(res)}
  end

  defp handle_rpc_multi_call({_responses, errors}, action, _) do
    wrap_error Nebulex.Error,
      reason: {:rpc, {:unexpected_errors, errors}},
      module: __MODULE__,
      action: action
  end

  @doc false
  def format_error({:rpc, {:unexpected_errors, errors}}, opts) do
    action = Keyword.fetch!(opts, :action)

    formatted_errors =
      Enum.map_join(errors, "\n\n", fn {{:error, reason}, node} ->
        ("Node #{node}:\n\n" <> Exception.format(:error, reason))
        |> String.replace("\n", "\n    ")
      end)

    """
    RPC multicall #{action} got unexpected errors.

    #{formatted_errors}
    """
  end
end
