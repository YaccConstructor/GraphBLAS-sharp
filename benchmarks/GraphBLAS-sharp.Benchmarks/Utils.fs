namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open GraphBLAS.FSharp

module GraphReader =
    let readMtx (pathToGraph: string) =
        use streamReader = new StreamReader(pathToGraph)

        while streamReader.Peek() = int '%' do
            streamReader.ReadLine() |> ignore

        let matrixInfo =
            streamReader.ReadLine().Split(' ')
            |> Array.map int

        let (nrows, ncols, nnz) =
            matrixInfo.[0], matrixInfo.[1], matrixInfo.[2]

        [0 .. nnz - 1]
        |> List.map
            (fun _ ->
                streamReader.ReadLine().Split(' ')
                |> (fun line -> int line.[0], int line.[1], float32 line.[2]))
        |> List.toArray
        |> Array.sortBy (fun (row, _, _) -> row)
        |> Array.unzip3
        |>
            fun (rows, cols, values) ->
                let c f x y = f y x
                let rows = rows |> Array.map (c (-) 1)
                let cols = cols |> Array.map (c (-) 1)
                {
                    Rows = rows
                    Columns = cols
                    Values = values
                    RowCount = nrows
                    ColumnCount = ncols
                }
