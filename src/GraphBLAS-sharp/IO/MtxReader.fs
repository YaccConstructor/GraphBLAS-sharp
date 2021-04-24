namespace GraphBLAS.FSharp.IO

open System.IO
open GraphBLAS.FSharp
open System

type MtxReader(pathToFile: string) =
    let mutable object = Matrix
    let mutable format = Coordinate
    let mutable field = Real
    let mutable symmetry = General

    do
        use streamReader = new StreamReader(pathToFile)
        let header = streamReader.ReadLine().Split(' ')
        object <- MtxObject.FromString header.[1]
        format <- MtxFormat.FromString header.[2]
        field <- MtxField.FromString header.[3]
        symmetry <- MtxSymmetry.FromString header.[4]

    member this.ReadMatrixReal() : GraphblasEvaluation<Matrix<float32>> =
        if object <> Matrix then failwith "Object is not matrix"
        if field <> Real then failwith "Field is not real"

        use streamReader = new StreamReader(pathToFile)
        while streamReader.Peek() = int '%' do
            streamReader.ReadLine() |> ignore

        graphblas {
            let ifCoordinate () =
                let size =
                    streamReader.ReadLine().Split(' ')
                    |> Array.map int

                let mutable n = size.[0]
                let mutable m = size.[1]
                let mutable nnz = size.[2]

                let pack x y = (uint64 x <<< 32) ||| (uint64 y)
                let unpack x = int ((x &&& 0xFFFFFFFF0000000UL) >>> 32), int (x &&& 0xFFFFFFFUL)

                let sortedData =
                    [0 .. nnz - 1]
                    |> List.map (fun _ -> streamReader.ReadLine().Split(' '))
                    |> Array.ofList
                    |> Array.Parallel.map
                        (fun line ->
                            let i = int line.[0]
                            let j = int line.[1]
                            let v = float32 line.[2]
                            struct(pack i j, v)
                        )
                    |> Array.sortBy (fun struct(packedIndex, _) -> packedIndex)

                let rows = Array.zeroCreate sortedData.Length
                let cols = Array.zeroCreate sortedData.Length
                let values = Array.zeroCreate sortedData.Length

                Array.Parallel.iteri (fun i struct(packedIndex, value) ->
                    let (rowIdx, columnIdx) = unpack packedIndex
                    // in mtx indecies start at 1
                    rows.[i] <- rowIdx - 1
                    cols.[i] <- columnIdx - 1
                    values.[i] <- value
                ) sortedData

                MatrixCOO {
                    Rows = rows
                    Columns = cols
                    Values = values
                    RowCount = n
                    ColumnCount = m
                }

            return
                match format with
                | Coordinate -> ifCoordinate ()
                | Array -> failwith "Unsupported matrix format"
        }

and private MtxObject =
    | Matrix
    | Vector
    static member FromString str =
        match str with
        | "matrix" -> Matrix
        | "vector" -> Vector
        | _ -> failwithf "Unsupported mtx object %s" str

and private MtxFormat =
    | Coordinate
    | Array
    static member FromString str =
        match str with
        | "coordinate" -> Coordinate
        | "array" -> Array
        | _ -> failwithf "Unsupported mtx format %s" str

and private MtxField =
    | Real
    | Double
    | Complex
    | Integer
    | Pattern
    static member FromString str =
        match str with
        | "real" -> Real
        | "double" -> Double
        | "complex" -> Complex
        | "integer" -> Integer
        | "pattern" -> Pattern
        | _ -> failwithf "Unsupported mtx field %s" str

and private MtxSymmetry =
    | General
    | Symmetric
    | SkewSymmetric
    | Hermitian
    static member FromString str =
        match str with
        | "general" -> General
        | "symmetric" -> Symmetric
        | "skew-symmetric" -> SkewSymmetric
        | "hermitian" -> Hermitian
        | _ -> failwithf "Unsupported mtx symmetry %s" str
