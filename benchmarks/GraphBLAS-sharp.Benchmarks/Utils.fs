namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open GraphBLAS.FSharp

type MtxFormat = {
    Object: string
    Format: string
    Field: string
    Symmetry: string
    Size: int[]
    Data: string[] list
}
with
    member this.RowCount = this.Size.[0]
    member this.ColumnCount = this.Size.[1]

module GraphReader =
    let readMtx (pathToGraph: string) =
        use streamReader = new StreamReader(pathToGraph)

        let header = streamReader.ReadLine().Split(' ')
        let object = header.[1]
        let format = header.[2]
        let field = header.[3]
        let symmetry = header.[4]

        while streamReader.Peek() = int '%' do
            streamReader.ReadLine() |> ignore

        let size =
            streamReader.ReadLine().Split(' ')
            |> Array.map int

        let len =
            match format with
            | "array" -> size.[0] * size.[1]
            | "coordinate" -> size.[2]
            | _ -> failwith "Unsupported matrix format"

        let data =
            [0 .. len - 1]
            |> List.map (fun _ -> streamReader.ReadLine().Split(' '))

        {
            Object = object
            Format = format
            Field = field
            Symmetry = symmetry
            Size = size
            Data = data
        }

type ValueProvider<'a> =
    | FromUnit of (unit -> 'a)
    | FromString of (string -> 'a)

module Utils =
    let getFullPathToMatrix matrixFilename =
        Path.Combine [|
            __SOURCE_DIRECTORY__
            "Datasets"
            "EWiseAddDatasets"
            matrixFilename
        |]

    let makeCOO (mtx: MtxFormat) (valueProvider: ValueProvider<'a>) =
        mtx.Data
        |> List.map
            (fun line ->
                let value =
                    match valueProvider with
                    | FromUnit get -> get ()
                    | FromString get -> get line.[2]

                (int line.[0], int line.[1], value)
            )
        |> List.toArray
        |> Array.sortBy (fun (row, col, _) -> row, col)
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
                    RowCount = mtx.RowCount
                    ColumnCount = mtx.ColumnCount
                }

    let transposeCOO (matrix: COOFormat<'a>) =
        (matrix.Rows, matrix.Columns, matrix.Values)
        |||> Array.zip3
        |> Array.sortBy (fun (row, col, value) -> col, row)
        |> Array.unzip3
        |>
            fun (rows, cols, values) ->
                {
                    Rows = cols
                    Columns = rows
                    Values = values
                    RowCount = matrix.ColumnCount
                    ColumnCount = matrix.RowCount
                }
