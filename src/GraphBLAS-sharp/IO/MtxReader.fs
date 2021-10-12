namespace GraphBLAS.FSharp.IO

open System.IO
open GraphBLAS.FSharp
open System

type MtxReader(pathToFile: string) =
    let mutable object = MtxMatrix
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

    member this.Object = object
    member this.Format = format
    member this.Field = field
    member this.Symmetry = symmetry

    override this.ToString() = Path.GetFileName pathToFile

    member this.ReadMatrixShape() =
        use streamReader = new StreamReader(pathToFile)

        while streamReader.Peek() = int '%' do
            streamReader.ReadLine() |> ignore

        let size =
            streamReader.ReadLine().Split(' ')
            |> Array.map int

        let nrows = size.[0]
        let ncols = size.[1]
        let nnz = size.[2]

        {| RowCount = nrows
           ColumnCount = ncols
           Nnz = nnz |}

    member this.ReadMatrixReal(converter: string -> 'a) : Matrix<'a> =
        if object <> MtxMatrix then
            failwith "Object is not matrix"

        if field <> Real then
            failwith "Field is not real"

        use streamReader = new StreamReader(pathToFile)

        while streamReader.Peek() = int '%' do
            streamReader.ReadLine() |> ignore

        let matrixFromCoordinateFormat () =
            let size =
                streamReader.ReadLine().Split(' ')
                |> Array.map int

            let n = size.[0]
            let m = size.[1]
            let nnz = size.[2]

            let pack x y = (uint64 x <<< 32) ||| (uint64 y)

            let unpack x =
                int ((x &&& 0xFFFFFFFF0000000UL) >>> 32), int (x &&& 0xFFFFFFFUL)

            let sortedData =
                [ 0 .. nnz - 1 ]
                |> List.map (fun _ -> streamReader.ReadLine().Split(' '))
                |> Array.ofList
                |> Array.Parallel.map
                    (fun line ->
                        let i = int line.[0]
                        let j = int line.[1]
                        let v = converter line.[2]
                        struct (pack i j, v))
                |> Array.sortBy (fun struct (packedIndex, _) -> packedIndex)

            let rows = Array.zeroCreate sortedData.Length
            let cols = Array.zeroCreate sortedData.Length
            let values = Array.zeroCreate sortedData.Length

            Array.Parallel.iteri
                (fun i struct (packedIndex, value) ->
                    let (rowIdx, columnIdx) = unpack packedIndex
                    // in mtx indecies start at 1
                    rows.[i] <- rowIdx - 1
                    cols.[i] <- columnIdx - 1
                    values.[i] <- value)
                sortedData

            MatrixCOO
                { Rows = rows
                  Columns = cols
                  Values = values
                  RowCount = n
                  ColumnCount = m }

        match format with
        | Coordinate -> matrixFromCoordinateFormat ()
        | Array -> failwith "Unsupported matrix format"

    member this.ReadMatrixBoolean(converter: string -> 'a) : Matrix<'a> =
        if object <> MtxMatrix then
            failwith "Object is not matrix"

        if field <> Pattern then
            failwith "Field is not boolean"

        use streamReader = new StreamReader(pathToFile)

        while streamReader.Peek() = int '%' do
            streamReader.ReadLine() |> ignore

        let matrixFromCoordinateFormat () =
            let size =
                streamReader.ReadLine().Split(' ')
                |> Array.map int

            let mutable n = size.[0]
            let mutable m = size.[1]
            let mutable nnz = size.[2]

            let pack x y = (uint64 x <<< 32) ||| (uint64 y)

            let unpack x =
                int ((x &&& 0xFFFFFFFF0000000UL) >>> 32), int (x &&& 0xFFFFFFFUL)

            let sortedData =
                [ 0 .. nnz - 1 ]
                |> List.map (fun _ -> streamReader.ReadLine().Split(' '))
                |> Array.ofList
                |> Array.Parallel.map
                    (fun line ->
                        let i = int line.[0]
                        let j = int line.[1]
                        let v = converter ""
                        struct (pack i j, v))
                |> Array.sortBy (fun struct (packedIndex, _) -> packedIndex)

            let rows = Array.zeroCreate sortedData.Length
            let cols = Array.zeroCreate sortedData.Length
            let values = Array.zeroCreate sortedData.Length

            Array.Parallel.iteri
                (fun i struct (packedIndex, value) ->
                    let (rowIdx, columnIdx) = unpack packedIndex
                    // in mtx indecies start at 1
                    rows.[i] <- rowIdx - 1
                    cols.[i] <- columnIdx - 1
                    values.[i] <- value)
                sortedData

            MatrixCOO
                { Rows = rows
                  Columns = cols
                  Values = values
                  RowCount = n
                  ColumnCount = m }

        match format with
        | Coordinate -> matrixFromCoordinateFormat ()
        | Array -> failwith "Unsupported matrix format"

    member this.ReadMatrixInteger(converter: string -> 'a) : Matrix<'a> =
        if object <> MtxMatrix then
            failwith "Object is not matrix"

        if field <> Integer then
            failwith "Field is not real"

        use streamReader = new StreamReader(pathToFile)

        while streamReader.Peek() = int '%' do
            streamReader.ReadLine() |> ignore

        let matrixFromCoordinateFormat () =
            let size =
                streamReader.ReadLine().Split(' ')
                |> Array.map int

            let n = size.[0]
            let m = size.[1]
            let nnz = size.[2]

            let pack x y = (uint64 x <<< 32) ||| (uint64 y)

            let unpack x =
                int ((x &&& 0xFFFFFFFF0000000UL) >>> 32), int (x &&& 0xFFFFFFFUL)

            let sortedData =
                [ 0 .. nnz - 1 ]
                |> List.map (fun _ -> streamReader.ReadLine().Split(' '))
                |> Array.ofList
                |> Array.Parallel.map
                    (fun line ->
                        let i = int line.[0]
                        let j = int line.[1]
                        let v = converter line.[2]
                        struct (pack i j, v))
                |> Array.sortBy (fun struct (packedIndex, _) -> packedIndex)

            let rows = Array.zeroCreate sortedData.Length
            let cols = Array.zeroCreate sortedData.Length
            let values = Array.zeroCreate sortedData.Length

            Array.Parallel.iteri
                (fun i struct (packedIndex, value) ->
                    let (rowIdx, columnIdx) = unpack packedIndex
                    // in mtx indecies start at 1
                    rows.[i] <- rowIdx - 1
                    cols.[i] <- columnIdx - 1
                    values.[i] <- value)
                sortedData

            MatrixCOO
                { Rows = rows
                  Columns = cols
                  Values = values
                  RowCount = n
                  ColumnCount = m }

        match format with
        | Coordinate -> matrixFromCoordinateFormat ()
        | Array -> failwith "Unsupported matrix format"

    member this.ReadMatrix(converter: string -> 'a) : Matrix<'a> =
        match field with
        | Real -> this.ReadMatrixReal(converter)
        | Pattern -> this.ReadMatrixBoolean(converter)
        | Integer -> this.ReadMatrixInteger(converter)
        | _ -> failwith "Not implemented"

and MtxObject =
    | MtxMatrix
    | MtxVector
    static member FromString str =
        match str with
        | "matrix" -> MtxMatrix
        | "vector" -> MtxVector
        | _ -> failwithf "Unsupported mtx object %s" str

and MtxFormat =
    | Coordinate
    | Array
    static member FromString str =
        match str with
        | "coordinate" -> Coordinate
        | "array" -> Array
        | _ -> failwithf "Unsupported mtx format %s" str

and MtxField =
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

and MtxSymmetry =
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
