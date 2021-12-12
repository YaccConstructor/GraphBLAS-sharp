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
        |> EWiseAdd.buffersCleanup processor EWiseAdd.getBuffersOfMatrixCSR

        rightCSR
        |> EWiseAdd.buffersCleanup processor EWiseAdd.getBuffersOfMatrixCSR

    [<IterationCleanup>]
    member this.IterCleanuo() =
        resultCSR
        |> EWiseAdd.buffersCleanup processor EWiseAdd.getBuffersOfMatrixCSR

    [<GlobalSetup>]
    member this.FormInputData() =
        processor.Error.Add(fun e -> failwithf "%A" e)

        let m, m1 =
            EWiseAdd.readMatrixAndSquaredCSR
                context
                this.InputMatrixReader
                this.DatasetFolder
                (float32)
                (fun _ -> Utils.nextSingle (System.Random()))
                toCSR

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
        |> EWiseAdd.buffersCleanup processor EWiseAdd.getBuffersOfMatrixCSR

        rightCSR
        |> EWiseAdd.buffersCleanup processor EWiseAdd.getBuffersOfMatrixCSR

    [<IterationCleanup>]
    member this.IterCleanuo() =
        resultCSR
        |> EWiseAdd.buffersCleanup processor EWiseAdd.getBuffersOfMatrixCSR

    [<GlobalSetup>]
    member this.FormInputData() =
        processor.Error.Add(fun e -> failwithf "%A" e)

        let m, m1 =
            EWiseAdd.readMatrixAndSquaredCSR
                context
                this.InputMatrixReader
                this.DatasetFolder
                (fun _ -> true)
                (fun _ -> true)
                toCSR

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
