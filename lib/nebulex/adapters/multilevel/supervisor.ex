defmodule Nebulex.Adapters.Multilevel.Supervisor do
  @moduledoc false

  use Supervisor

  ## API

  @doc false
  def start_link({name, children}) do
    Supervisor.start_link(__MODULE__, children, name: name)
  end

  ## Supervisor callback

  @impl true
  def init(children) do
    Supervisor.init(children, strategy: :rest_for_one)
  end
end
