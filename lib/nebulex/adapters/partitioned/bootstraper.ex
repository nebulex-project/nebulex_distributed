defmodule Nebulex.Adapters.Partitioned.Bootstraper do
  @moduledoc false
  use GenServer

  import Nebulex.Utils

  alias Nebulex.Distributed.Cluster
  alias Nebulex.Telemetry

  # State
  defstruct [:adapter_meta, :join_timeout]

  ## API

  @doc false
  def start_link({%{name: name}, _} = state) do
    GenServer.start_link(
      __MODULE__,
      state,
      name: camelize_and_concat([name, Bootstrap])
    )
  end

  ## GenServer Callbacks

  @impl true
  def init({adapter_meta, opts}) do
    # Trap exit signals to run cleanup job
    _ = Process.flag(:trap_exit, true)

    # Bootstrap started
    :ok = dispatch_telemetry_event(:started, adapter_meta)

    # Ensure joining the cluster when the cache supervision tree is started
    :ok = Cluster.join(adapter_meta.name)

    # Bootstrap joined the cache to the cluster
    :ok = dispatch_telemetry_event(:joined, adapter_meta)

    # Build initial state
    state = build_state(adapter_meta, opts)

    # Start bootstrap process
    {:ok, state, state.join_timeout}
  end

  @impl true
  def handle_info(message, state)

  def handle_info(:timeout, %__MODULE__{adapter_meta: adapter_meta} = state) do
    # Ensure it is always joined to the cluster
    :ok = Cluster.join(adapter_meta.name)

    # Bootstrap joined the cache to the cluster
    :ok = dispatch_telemetry_event(:joined, adapter_meta)

    {:noreply, state, state.join_timeout}
  end

  def handle_info({:EXIT, _from, reason}, %__MODULE__{adapter_meta: adapter_meta} = state) do
    # Bootstrap received exit signal
    :ok = dispatch_telemetry_event(:exit, adapter_meta, %{reason: reason})

    {:stop, reason, state}
  end

  @impl true
  def terminate(reason, %__MODULE__{adapter_meta: adapter_meta}) do
    # Ensure leaving the cluster when the cache stops
    :ok = Cluster.leave(adapter_meta.name)

    # Bootstrap stopped or terminated
    :ok = dispatch_telemetry_event(:stopped, adapter_meta, %{reason: reason})
  end

  ## Private Functions

  defp build_state(adapter_meta, opts) do
    # Join timeout to ensure it is always joined to the cluster
    join_timeout = Keyword.fetch!(opts, :join_timeout)

    %__MODULE__{adapter_meta: adapter_meta, join_timeout: join_timeout}
  end

  defp dispatch_telemetry_event(event, adapter_meta, meta \\ %{}) do
    Telemetry.execute(
      adapter_meta.telemetry_prefix ++ [:bootstrap, event],
      %{system_time: System.system_time()},
      Map.merge(meta, %{
        adapter_meta: adapter_meta,
        cluster_nodes: Cluster.get_nodes(adapter_meta.name)
      })
    )
  end
end
