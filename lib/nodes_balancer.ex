defmodule NodesBalancer do
  require Logger
  use GenServer

  @moduledoc """
  Documentation for NodesBalancer.
  """

  @doc """
  Hello world.

  ## Examples

      iex> NodesBalancer.hello()
      :world

  """

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    send(self(), :synchronize)
    {:ok, %{main: nil, other: []}}
  end

  @impl true
  def handle_call(:get_main, _, state) do
    main = Map.get(state, :main)
    {:reply, main, state}
  end

  @impl true
  def handle_call(:get_state, _, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:join, ref}, _, %{main: main, other: other} = state) do
    other =
      if ref == main do
        other
      else
        start_monitoring(ref)
        Enum.uniq([ref | other])
      end

    new_state = Map.put(state, :other, other)
    {:reply, new_state, new_state}
  end

  @impl true
  def handle_info(:synchronize, state) do
    Logger.info("Start synchronize. Current state #{inspect(state)}. I am #{node()}",
      ansi_color: :green
    )

    new_state =
      case Node.list() do
        [] ->
          Map.put(state, :main, node())

        nodes ->
          case mylticall(nodes, :join) do
            :retry -> retry_sync(state)
            resp -> resp
          end
      end

    [new_state[:main] | new_state[:other]]
    |> start_monitoring()

    Logger.info("Sync complete. New state #{inspect(new_state)}. I am #{node()}",
      ansi_color: :green
    )

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:nodedown, down_node}, %{main: main, other: other}) do
    new_state =
      if down_node == main do
        [new_main | new_other] = other
        %{main: new_main, other: new_other}
      else
        %{main: main, other: other -- [down_node]}
      end

    Logger.warn(
      "Node #{down_node} fell!\n\t Main node: #{new_state.main}.\n\t Nodes left: #{
        inspect(new_state.other)
      }"
    )

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _, _, pid, :normal}, state) do
    Logger.info("Process #{inspect(pid)} successful completed", ansi_color: :green)
    {:noreply, state}
  end

  def handle_info(reason, state) do
    Logger.error(inspect(reason))
    {:noreply, state}
  end

  def get_state do
    GenServer.call(__MODULE__, :get_state)
  end

  def join(ref) do
    GenServer.call(__MODULE__, {:join, ref})
  end

  def who_is_main() do
    GenServer.call(__MODULE__, :get_main)
  end

  def count_all_children(module, params \\ []) do
    mylticall(Node.list(), module, :count_children, params)
  end

  def is_main(noda) do
    GenServer.call(__MODULE__, :get_main) == noda
  end

  def call(noda, module, func, params) do
    case :rpc.call(noda, module, func, params) do
      {:badrpc, reason} -> Logger.error(inspect(reason))
      replies -> replies
    end
  end

  defp mylticall(nodes, :join = func) do
    {replies, _} = :rpc.multicall(nodes, ExTasksDispatcher.Workers.NodesBalancer, func, [node()])

    replies
    |> List.first()
    |> case do
      %{main: _, other: _} = response ->
        response

      reason ->
        Logger.error(inspect(reason))
        :retry
    end
  end

  def mylticall([], _, _, _), do: []

  def mylticall(nodes, module, func, params) do
    {replies, _} = :rpc.multicall(nodes, module, func, params)
    replies |> List.first()
  end

  defp retry_sync(state) do
    Process.send_after(self(), :synchronize, 2000)

    if is_nil(state[:main]) do
      Map.put(state, :main, node())
    else
      state
    end
  end

  defp start_monitoring(nil) do
    Logger.error("Can't monitoring nil node")
  end

  defp start_monitoring(nodes) when is_list(nodes) do
    Enum.each(nodes, &start_monitoring/1)
  end

  defp start_monitoring(noda) do
    if node() == noda do
      Logger.info("It's the same node, I just ignore it #{noda}", ansi_color: :green)
    else
      Logger.info("Started monitoring the node #{noda}", ansi_color: :green)
      :erlang.monitor_node(noda, true)
    end
  end
end
