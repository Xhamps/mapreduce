defmodule Mapper do

  def map(line, partition) do
    send(partition, {:process_put, self()})

    line
    |> String.split(" ")
    |> Enum.each(& send(partition, {:value_put, &1}))
  end
end
