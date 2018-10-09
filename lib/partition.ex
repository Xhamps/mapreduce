defmodule Partition do
  require Reducer
  require OutputWriter

  def start_link do
    Task.start_link(fn -> loop([], []) end)
  end

  defp loop(processes, values) do
    mailbox_length = elem(Process.info(self(), :message_queue_len), 1)

    if (mailbox_length === 0) do
      mapper_check(processes, clean(values))
    end

    receive do
      {:process_put, caller} -> loop([caller | processes], values)
      {:value_put, key} -> loop(processes, [{String.to_atom(key), 1} | values])
      error -> IO.puts :stderr, "Partition Error: #{error}"
    end
  end

  defp mapper_check(processes, values) do
    check = Enum.filter(processes, &Process.alive?/1)

    uniques = Enum.uniq(Keyword.keys(values))

    if (length(check) == 0 && length(uniques) != 0) do
      output_write = elem(OutputWriter.start_link, 1)

      Enum.each(uniques, fn unique -> spawn(fn -> reduce_spawn(unique, values, output_write) end) end)
    end
  end

  defp clean(values) do
    values
    |> Keyword.delete(String.to_atom(~s(\s)))
    |> Keyword.delete(String.to_atom(""))
  end

  defp reduce_spawn(unique, values, output_write) do
    values
    |> Keyword.take([unique])
    |> Keyword.to_list
    |> Reducer.reduce(output_write)
  end
end
