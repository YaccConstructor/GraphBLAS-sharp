namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open GraphBLAS.FSharp

type MtxShape = {
    Filename: string
    Object: string
    Format: string
    Field: string
    Symmetry: string
    Size: int[]
}
with
    member this.RowCount = this.Size.[0]
    member this.ColumnCount = this.Size.[1]
    override this.ToString() =
        sprintf "%s" <| Path.GetFileNameWithoutExtension this.Filename

type MtxFormat = {
    Shape: MtxShape
    Data: string[] list
}

module GraphReader =
    let readShapeMtx (pathToGraph: string) =
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

        {
            Filename = pathToGraph |> Path.GetFileName
            Object = object
            Format = format
            Field = field
            Symmetry = symmetry
            Size = size
        }

    let readMtx (pathToGraph: string) =
        let meta = readShapeMtx pathToGraph
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
            match meta.Format with
            | "array" -> meta.Size.[0] * meta.Size.[1]
            | "coordinate" -> meta.Size.[2]
            | _ -> failwith "Unsupported matrix format"

        let data =
            [0 .. len - 1]
            |> List.map (fun _ -> streamReader.ReadLine().Split(' '))

        {
            Shape = meta
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

    let pack x y = (uint64 x <<< 32) ||| (uint64 y)
    let unpack x = (int ((x &&& 0xFFFFFFFF0000000UL) >>> 32)), (int (x &&& 0xFFFFFFFUL))

    let makeCOO (mtx: MtxFormat) (valueProvider: ValueProvider<'a>) =
        printfn "Start make COO"
        mtx.Data
        |> List.toArray
        |> Array.Parallel.map
              (fun line ->
                 let value =
                     match valueProvider with
                     | FromUnit get -> get ()
                     | FromString get -> get line.[2]
                 struct (pack (int line.[0]) (int line.[1]), value)
              )
        |> Array.sortBy (fun struct (p, _) -> p)
        |> fun data ->
               let rows = Array.zeroCreate data.Length
               let cols = Array.zeroCreate data.Length
               let values = Array.zeroCreate data.Length
               data 
               |> Array.Parallel.iteri 
                      (fun i struct(p,v) -> 
                          let r,c = unpack p
                          rows.[i] <- r
                          cols.[i] <- c
                          values.[i] <- v
                      )
               let c f x y = f y x
               let rows = rows |> Array.map (c (-) 1)
               let cols = cols |> Array.map (c (-) 1)
               printfn "kek"
               {
                   Rows = rows
                   Columns = cols
                   Values = values
                   RowCount = mtx.Shape.RowCount
                   ColumnCount = mtx.Shape.ColumnCount
               }

    let transposeCOO (matrix: COOFormat<'a>) =
        printfn "Start transpose COO"
        Array.map3 (fun c r v -> struct ((pack c r), v)) matrix.Columns matrix.Rows matrix.Values
        |> Array.sortBy (fun struct (p, value) -> p)
        |>
            fun data ->
                let rows = Array.zeroCreate data.Length
                let cols = Array.zeroCreate data.Length
                let values = Array.zeroCreate data.Length
                data 
                |> Array.Parallel.iteri 
                       (fun i struct(p, v) -> 
                            let r,c = unpack p
                            rows.[i] <- r
                            cols.[i] <- c
                            values.[i] <- v)
                {
                    Rows = rows
                    Columns = cols
                    Values = values
                    RowCount = matrix.ColumnCount
                    ColumnCount = matrix.RowCount
                }