defmodule InputReader do
  require Mapper

  def reader(file, partition) do
    case File.read(file) do
      {:ok, body} -> body |> parse_body |> Enum.each(& each_line(&1, partition))
      {:error, reason} -> IO.puts :stderr, "File Error: #{reason}"
    end
  end

  defp parse_body(body) do
    body
    |> String.trim
    |> (& Regex.split(~r/\r|\n|\r\n/, &1)).()
  end

  defp each_line(line, partition) do
    spawn(fn -> Mapper.map(line, partition) end)
  end
end
