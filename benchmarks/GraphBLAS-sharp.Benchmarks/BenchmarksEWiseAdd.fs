namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open System.Text.RegularExpressions
open GraphBLAS.FSharp
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open Brahma.FSharp.OpenCL
open OpenCL.Net

type Config() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", (fun matrix -> matrix.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixShapeColumn("ColumnCount", (fun matrix -> matrix.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixShapeColumn(
                "NNZ",
                fun matrix ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

[<IterationCount(5)>]
[<WarmupCount(3)>]
[<Config(typeof<Config>)>]
type EWiseAddBenchmarks() =
    [<ParamsSource("AvaliableContexts")>]
    member val OclContext = Unchecked.defaultof<ClContext> with get, set

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader> with get, set

    member this.Processor = this.OclContext.Provider.CommandQueue

    member val WorkGroupSize = 128

    member val DatasetFolder = "EWiseAdd"

    static member AvaliableContexts = Utils.avaliableContexts

    static member getBuffersOfMatrixCSR(matrix: Backend.CSRMatrix<'a>) =
        matrix.RowPointers, matrix.Columns, matrix.Values

    static member getBuffersOfMatrixCOO(matrix: Backend.COOMatrix<'a>) =
        matrix.Rows, matrix.Columns, matrix.Values

    static member buffersCleanup (processor: MailboxProcessor<_>) getBuffersFun matrix =
        let buffers = getBuffersFun matrix

        match buffers with
        | b1, b2, b3 ->
            processor.Post(Msg.CreateFreeMsg<_>(b1))
            processor.Post(Msg.CreateFreeMsg<_>(b2))
            processor.Post(Msg.CreateFreeMsg<_>(b3))

        processor.PostAndReply(Msg.MsgNotifyMe)

    member this.readMatrixAndSquaredCSR (context: Brahma.FSharp.OpenCL.ClContext) converter converterBool toCSR =
        let squaredMatrixReader =
            MtxReader(Utils.getFullPathToMatrix this.DatasetFolder ("squared_" + this.InputMatrixReader.ToString()))

        let matrixWrapped, matrixWrapped1 =
            match this.InputMatrixReader.Field, squaredMatrixReader.Field with
            | Pattern, Pattern ->
                this.InputMatrixReader.ReadMatrixBoolean(converterBool),
                squaredMatrixReader.ReadMatrixBoolean(converterBool)
            | Pattern, _ ->
                this.InputMatrixReader.ReadMatrixBoolean(converterBool), squaredMatrixReader.ReadMatrix(converter)
            | _, Pattern ->
                this.InputMatrixReader.ReadMatrix(converter), squaredMatrixReader.ReadMatrixBoolean(converterBool)
            | _, _ -> this.InputMatrixReader.ReadMatrix(converter), squaredMatrixReader.ReadMatrix(converter)

        let matrix, matrix1 =
            match matrixWrapped, matrixWrapped1 with
            | MatrixCOO matrix, MatrixCOO matrix1 ->
                let leftRows = context.CreateClArray(matrix.Rows)

                let leftCols = context.CreateClArray matrix.Columns

                let leftVals = context.CreateClArray matrix.Values

                let left =
                    { Backend.COOMatrix.RowCount = matrix.RowCount
                      Backend.COOMatrix.ColumnCount = matrix.ColumnCount
                      Backend.COOMatrix.Rows = leftRows
                      Backend.COOMatrix.Columns = leftCols
                      Backend.COOMatrix.Values = leftVals }

                let rightRows = context.CreateClArray matrix1.Rows

                let rightCols = context.CreateClArray matrix1.Columns

                let rightVals = context.CreateClArray matrix1.Values

                let right =
                    { Backend.COOMatrix.RowCount = matrix1.RowCount
                      Backend.COOMatrix.ColumnCount = matrix1.ColumnCount
                      Backend.COOMatrix.Rows = rightRows
                      Backend.COOMatrix.Columns = rightCols
                      Backend.COOMatrix.Values = rightVals }

                right, left
            | _ -> failwith "Unsupported matrix format"

        toCSR matrix, toCSR matrix1

type EWiseAddBenchmarks4Float32COO() as this =
    inherit EWiseAddBenchmarks()

    let _eWiseAdd =
        Backend.COOMatrix.eWiseAdd (this.OclContext) <@ (+) @> // 128 //this.Processor

    let mutable leftCOO =
        Unchecked.defaultof<Backend.COOMatrix<float32>>

    let mutable rightCOO =
        Unchecked.defaultof<Backend.COOMatrix<float32>>

    let mutable resultCOO =
        Unchecked.defaultof<Backend.COOMatrix<float32>>

    member val FirstMatrix = Unchecked.defaultof<COOMatrix<float32>> with get, set
    member val SecondMatrix = Unchecked.defaultof<COOMatrix<float32>> with get, set

    member this.Processor = this.OclContext.Provider.CommandQueue

    member this.eWiseAdd =
        Backend.COOMatrix.eWiseAdd (this.OclContext) <@ (+) @> this.WorkGroupSize this.Processor

    [<IterationCleanup>]
    member this.ClearBuffers() =
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCOO.Rows))
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCOO.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCOO.Values))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCOO.Rows))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCOO.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCOO.Values))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCOO.Rows))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCOO.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCOO.Values))
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

    [<GlobalSetup>]
    member this.FormInputData() =
        this.Processor.Error.Add(fun e -> failwithf "%A" e)

        let matrixWrapped =
            match this.InputMatrixReader.Field with
            | Real
            | Integer -> this.InputMatrixReader.ReadMatrix(float32)
            | Pattern -> this.InputMatrixReader.ReadMatrixBoolean(fun _ -> Utils.nextSingle (System.Random()))
            | _ -> failwith "Unsupported matrix format"

        let matrix, matrix1 =
            match matrixWrapped with
            | MatrixCOO m ->
                let mnMatrix =
                    MathNet.Numerics.LinearAlgebra.Matrix<float32>.Build.SparseFromCoordinateFormat
                        (m.RowCount, m.ColumnCount, m.Values.Length, m.Rows, m.Columns, m.Values)

                let mnMatrix1 = mnMatrix.Multiply(mnMatrix)

                let storage =
                    MathNet.Numerics.LinearAlgebra.Storage.SparseCompressedRowMatrixStorage.OfMatrix(mnMatrix1.Storage)

                let cols = storage.ColumnIndices
                let vals = storage.Values

                let rows =
                    Utils.rowPointers2rowIndices storage.RowPointers

                let m1 =
                    { Rows = rows
                      Columns = cols
                      Values = vals
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount }

                m, m1
            | _ -> failwith "Unsupported matrix format"

        this.FirstMatrix <- matrix
        this.SecondMatrix <- matrix1

    [<IterationSetup>]
    member this.BuildCOO() =
        let leftRows =
            this.OclContext.CreateClArray this.FirstMatrix.Rows

        let leftCols =
            this.OclContext.CreateClArray this.FirstMatrix.Columns

        let leftVals =
            this.OclContext.CreateClArray this.FirstMatrix.Values

        leftCOO <-
            { RowCount = this.FirstMatrix.RowCount
              ColumnCount = this.FirstMatrix.ColumnCount
              Rows = leftRows
              Columns = leftCols
              Values = leftVals }

        let rightRows =
            this.OclContext.CreateClArray this.SecondMatrix.Rows

        let rightCols =
            this.OclContext.CreateClArray this.SecondMatrix.Columns

        let rightVals =
            this.OclContext.CreateClArray this.SecondMatrix.Values

        rightCOO <-
            { RowCount = this.SecondMatrix.RowCount
              ColumnCount = this.SecondMatrix.ColumnCount
              Rows = rightRows
              Columns = rightCols
              Values = rightVals }

    [<Benchmark>]
    member this.EWiseAdditionCOOFloat32() =
        resultCOO <- this.eWiseAdd leftCOO rightCOO

    static member InputMatricesProvider =
        "EWiseAddBenchmarks4Float32COO.txt"
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                printfn "%A" matrixFilename

                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix "EWiseAdd" matrixFilename)
                | _ -> failwith "Unsupported matrix format")

type EWiseAddBenchmarks4BoolCOO() =
    inherit EWiseAddBenchmarks()

    let mutable leftCOO =
        Unchecked.defaultof<Backend.COOMatrix<bool>>

    let mutable rightCOO =
        Unchecked.defaultof<Backend.COOMatrix<bool>>

    let mutable resultCOO =
        Unchecked.defaultof<Backend.COOMatrix<bool>>

    member val FirstMatrix = Unchecked.defaultof<COOMatrix<bool>> with get, set
    member val SecondMatrix = Unchecked.defaultof<COOMatrix<bool>> with get, set

    member this.Processor = this.OclContext.Provider.CommandQueue
    // To do: set the correct workGroupSize
    member this.eWiseAdd =
        Backend.COOMatrix.eWiseAdd (this.OclContext) <@ (||) @> 128 this.Processor

    [<IterationCleanup>]
    member this.ClearBuffers() =
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCOO.Rows))
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCOO.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCOO.Values))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCOO.Rows))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCOO.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCOO.Values))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCOO.Rows))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCOO.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCOO.Values))
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

    [<GlobalSetup>]
    member this.FormInputData() =
        this.Processor.Error.Add(fun e -> failwithf "%A" e)

        let matrixWrapped =
            this.InputMatrixReader.ReadMatrix(fun _ -> true)

        let matrix, matrix1 =
            match matrixWrapped with
            | MatrixCOO m ->
                let mnMatrix =
                    MathNet.Numerics.LinearAlgebra.Matrix<float32>.Build.SparseFromCoordinateFormat
                        (m.RowCount,
                         m.ColumnCount,
                         m.Values.Length,
                         m.Rows,
                         m.Columns,
                         Array.map (fun _ -> 1.0f) m.Values)

                let mnMatrix1 = mnMatrix.Multiply(mnMatrix)

                let storage =
                    MathNet.Numerics.LinearAlgebra.Storage.SparseCompressedRowMatrixStorage.OfMatrix(mnMatrix1.Storage)

                let cols = storage.ColumnIndices
                let vals = Array.map (fun _ -> true) storage.Values

                let rows =
                    Utils.rowPointers2rowIndices storage.RowPointers

                let m1 =
                    { Rows = rows
                      Columns = cols
                      Values = vals
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount }

                m, m1
            | _ -> failwith "Unsupported matrix format"

        this.FirstMatrix <- matrix
        this.SecondMatrix <- matrix1

    [<IterationSetup>]
    member this.BuildCOO() =
        let leftRows =
            this.OclContext.CreateClArray this.FirstMatrix.Rows

        let leftCols =
            this.OclContext.CreateClArray this.FirstMatrix.Columns

        let leftVals =
            this.OclContext.CreateClArray this.FirstMatrix.Values

        leftCOO <-
            { RowCount = this.FirstMatrix.RowCount
              ColumnCount = this.FirstMatrix.ColumnCount
              Rows = leftRows
              Columns = leftCols
              Values = leftVals }

        let rightRows =
            this.OclContext.CreateClArray this.SecondMatrix.Rows

        let rightCols =
            this.OclContext.CreateClArray this.SecondMatrix.Columns

        let rightVals =
            this.OclContext.CreateClArray this.SecondMatrix.Values

        rightCOO <-
            { RowCount = this.SecondMatrix.RowCount
              ColumnCount = this.SecondMatrix.ColumnCount
              Rows = rightRows
              Columns = rightCols
              Values = rightVals }

    [<Benchmark>]
    member this.EWiseAdditionCOOBool() =
        resultCOO <- this.eWiseAdd leftCOO rightCOO

    static member InputMatricesProvider =
        "EWiseAddBenchmarks4BoolCOO.txt"
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix "EWiseAdd" matrixFilename)
                | _ -> failwith "Unsupported matrix format")

type EWiseAddBenchmarks4Float32CSR() as this =
    inherit EWiseAddBenchmarks()

    let context =
        ClContext(ClPlatform.Nvidia, ClDeviceType.GPU)

    let processor = context.Provider.CommandQueue

    let mutable leftCSR =
        Unchecked.defaultof<Backend.CSRMatrix<float32>>

    let mutable rightCSR =
        Unchecked.defaultof<Backend.CSRMatrix<float32>>

    let mutable resultCSR =
        Unchecked.defaultof<Backend.CSRMatrix<float32>>

    let eWiseAdd =
        Backend.CSRMatrix.eWiseAdd context <@ (+) @> this.WorkGroupSize

    let toCSR =
        Backend.COOMatrix.toCSR context this.WorkGroupSize processor

    [<GlobalCleanup>]
    member this.GlobalCleanup() =
        leftCSR
        |> EWiseAddBenchmarks.buffersCleanup processor EWiseAddBenchmarks.getBuffersOfMatrixCSR

        rightCSR
        |> EWiseAddBenchmarks.buffersCleanup processor EWiseAddBenchmarks.getBuffersOfMatrixCSR

    [<IterationCleanup>]
    member this.IterCleanup() =
        resultCSR
        |> EWiseAddBenchmarks.buffersCleanup processor EWiseAddBenchmarks.getBuffersOfMatrixCSR

    [<GlobalSetup>]
    member this.FormInputData() =
        processor.Error.Add(fun e -> failwithf "%A" e)

        let m, m1 =
            this.readMatrixAndSquaredCSR context (float32) (fun _ -> Utils.nextSingle (System.Random())) toCSR

        leftCSR <- m
        rightCSR <- m1

    [<Benchmark>]
    member this.EWiseAdditionCSRFloat32() =
        resultCSR <- eWiseAdd processor leftCSR rightCSR

    static member InputMatricesProvider =
        "EWiseAddBenchmarks4Float32CSR.txt"
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix "EWiseAdd" matrixFilename)
                | _ -> failwith "Unsupported matrix format")

type EWiseAddBenchmarks4BoolCSR() as this =
    inherit EWiseAddBenchmarks()

    let context =
        ClContext(ClPlatform.Nvidia, ClDeviceType.GPU)

    let processor = context.Provider.CommandQueue

    let mutable leftCSR =
        Unchecked.defaultof<Backend.CSRMatrix<bool>>

    let mutable rightCSR =
        Unchecked.defaultof<Backend.CSRMatrix<bool>>

    let mutable resultCSR =
        Unchecked.defaultof<Backend.CSRMatrix<bool>>

    let eWiseAdd =
        Backend.CSRMatrix.eWiseAdd context <@ (||) @> this.WorkGroupSize

    let toCSR =
        Backend.COOMatrix.toCSR context this.WorkGroupSize processor

    [<GlobalCleanup>]
    member this.GlobalCleanup() =
        leftCSR
        |> EWiseAddBenchmarks.buffersCleanup processor EWiseAddBenchmarks.getBuffersOfMatrixCSR

        rightCSR
        |> EWiseAddBenchmarks.buffersCleanup processor EWiseAddBenchmarks.getBuffersOfMatrixCSR

    [<IterationCleanup>]
    member this.IterCleanup() =
        resultCSR
        |> EWiseAddBenchmarks.buffersCleanup processor EWiseAddBenchmarks.getBuffersOfMatrixCSR

    [<GlobalSetup>]
    member this.FormInputData() =
        processor.Error.Add(fun e -> failwithf "%A" e)

        let m, m1 =
            this.readMatrixAndSquaredCSR context (fun _ -> true) (fun _ -> true) toCSR

        leftCSR <- m
        rightCSR <- m1

    [<Benchmark>]
    member this.EWiseAdditionCSRBool() =
        resultCSR <- eWiseAdd processor leftCSR rightCSR

    static member InputMatricesProvider =
        "EWiseAddBenchmarks4BoolCSR.txt"
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix "EWiseAdd" matrixFilename)
                | _ -> failwith "Unsupported matrix format")
