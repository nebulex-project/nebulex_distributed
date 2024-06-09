defmodule Nebulex.Adapters.Distributed.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = pg_children()

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Nebulex.Adapters.Distributed.Supervisor
    )
  end

  if Code.ensure_loaded?(:pg) do
    defp pg_children do
      [%{id: :pg, start: {:pg, :start_link, [Nebulex.Cluster]}}]
    end
  else
    defp pg_children do
      []
    end
  end
end
