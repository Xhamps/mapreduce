defmodule Reducer do
  def reduce(tuples, output_write) do
    send(output_write, {:process_put, self()})
    case tuples do
      [] -> IO.puts :stderr, "Empty list"
      tuples -> send(output_write, {:value_put, "#{elem(hd(tuples), 0)} #{count(tuples)}"})
    end
  end

  defp count(tuples) do
    tuples
    |> Enum.reduce(0, fn ({_, v}, total) -> v + total end)
  end
end
