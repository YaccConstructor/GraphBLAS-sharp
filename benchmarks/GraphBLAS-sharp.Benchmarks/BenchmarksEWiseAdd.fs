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

    member val WorkGroupSize = 128

    //[<GlobalCleanup>]
    //member this.ClearContext() =
    //    let (ClContext context) = this.OclContext
    //    context.D

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

    let _eWiseAdd =
        Backend.CSRMatrix.eWiseAdd (this.OclContext) <@ (+) @>

    let mutable leftCSR =
        Unchecked.defaultof<Backend.CSRMatrix<float32>>

    let mutable rightCSR =
        Unchecked.defaultof<Backend.CSRMatrix<float32>>

    let mutable resultCSR =
        Unchecked.defaultof<Backend.CSRMatrix<float32>>

    member val FirstMatrix = Unchecked.defaultof<CSRMatrix<float32>> with get, set
    member val SecondMatrix = Unchecked.defaultof<CSRMatrix<float32>> with get, set

    member this.Processor = this.OclContext.Provider.CommandQueue

    member this.eWiseAdd =
        Backend.CSRMatrix.eWiseAdd (this.OclContext) <@ (+) @>

    [<IterationCleanup>]
    member this.ClearBuffers() =
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCSR.RowPointers))
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCSR.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCSR.Values))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCSR.RowPointers))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCSR.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCSR.Values))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCSR.RowPointers))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCSR.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCSR.Values))
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

    [<GlobalSetup>]
    member this.FormInputData() =
        this.Processor.Error.Add(fun e -> failwithf "%A" e)

        let SquaredMatrixReader =
            MtxReader(Utils.getFullPathToMatrix "EWiseAdd" ("squared_" + this.InputMatrixReader.ToString()))

        let matrixWrapped, matrixWrapped1 =
            match this.InputMatrixReader.Field, SquaredMatrixReader.Field with
            | Pattern, Pattern ->
                this.InputMatrixReader.ReadMatrixBoolean(fun _ -> Utils.nextSingle (System.Random())),
                SquaredMatrixReader.ReadMatrixBoolean(fun _ -> Utils.nextSingle (System.Random()))
            | Pattern, _ ->
                this.InputMatrixReader.ReadMatrixBoolean(fun _ -> Utils.nextSingle (System.Random())),
                SquaredMatrixReader.ReadMatrix(float32)
            | _, Pattern ->
                this.InputMatrixReader.ReadMatrix(float32),
                SquaredMatrixReader.ReadMatrixBoolean(fun _ -> Utils.nextSingle (System.Random()))
            | _, _ -> this.InputMatrixReader.ReadMatrix(float32), SquaredMatrixReader.ReadMatrix(float32)

        let matrix, matrix1 =
            match matrixWrapped, matrixWrapped1 with
            | MatrixCOO m, MatrixCOO m1 ->
                let mnMatrix =
                    MathNet.Numerics.LinearAlgebra.Matrix<float32>.Build.SparseFromCoordinateFormat
                        (m.RowCount, m.ColumnCount, m.Values.Length, m.Rows, m.Columns, m.Values)

                let mnMatrixCSR =
                    MathNet.Numerics.LinearAlgebra.Storage.SparseCompressedRowMatrixStorage.OfMatrix(mnMatrix.Storage)

                let mnMatrix1 =
                    MathNet.Numerics.LinearAlgebra.Matrix<float32>.Build.SparseFromCoordinateFormat
                        (m1.RowCount, m1.ColumnCount, m1.Values.Length, m1.Rows, m1.Columns, m1.Values)

                let mnMatrix1CSR =
                    MathNet.Numerics.LinearAlgebra.Storage.SparseCompressedRowMatrixStorage.OfMatrix(mnMatrix1.Storage)

                let m =
                    { CSRMatrix.ColumnCount = mnMatrixCSR.ColumnCount
                      RowCount = mnMatrixCSR.RowCount
                      ColumnIndices = mnMatrixCSR.ColumnIndices
                      RowPointers = mnMatrixCSR.RowPointers.[..mnMatrixCSR.RowPointers.Length - 2]
                      Values = mnMatrixCSR.Values }

                let m1 =
                    { CSRMatrix.ColumnCount = mnMatrix1CSR.ColumnCount
                      RowCount = mnMatrix1CSR.RowCount
                      ColumnIndices = mnMatrix1CSR.ColumnIndices
                      RowPointers = mnMatrix1CSR.RowPointers.[..mnMatrix1CSR.RowPointers.Length - 2]
                      Values = mnMatrix1CSR.Values }

                m, m1
            | _ -> failwith "Unsupported matrix format2"

        this.FirstMatrix <- matrix
        this.SecondMatrix <- matrix1

    [<IterationSetup>]
    member this.BuildCSR() =
        let leftRowPointers =
            this.OclContext.CreateClArray this.FirstMatrix.RowPointers

        let leftCols =
            this.OclContext.CreateClArray this.FirstMatrix.ColumnIndices

        let leftVals =
            this.OclContext.CreateClArray this.FirstMatrix.Values

        let left =
            { Backend.CSRMatrix.RowCount = this.FirstMatrix.RowCount
              Backend.CSRMatrix.ColumnCount = this.FirstMatrix.ColumnCount
              Backend.CSRMatrix.RowPointers = leftRowPointers
              Backend.CSRMatrix.Columns = leftCols
              Backend.CSRMatrix.Values = leftVals }

        leftCSR <- left

        let rightRowPointers =
            this.OclContext.CreateClArray this.SecondMatrix.RowPointers

        let rightCols =
            this.OclContext.CreateClArray this.SecondMatrix.ColumnIndices

        let rightVals =
            this.OclContext.CreateClArray this.SecondMatrix.Values

        let right =
            { Backend.CSRMatrix.RowCount = this.SecondMatrix.RowCount
              Backend.CSRMatrix.ColumnCount = this.SecondMatrix.ColumnCount
              Backend.CSRMatrix.RowPointers = rightRowPointers
              Backend.CSRMatrix.Columns = rightCols
              Backend.CSRMatrix.Values = rightVals }

        rightCSR <- right

    [<Benchmark>]
    member this.EWiseAdditionCSRFloat32() =
        resultCSR <- this.eWiseAdd this.WorkGroupSize this.Processor leftCSR rightCSR

    static member InputMatricesProvider =
        "EWiseAddBenchmarks4Float32CSR.txt"
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix "EWiseAdd" matrixFilename)
                | _ -> failwith "Unsupported matrix format")

type EWiseAddBenchmarks4BoolCSR() =
    inherit EWiseAddBenchmarks()

    let mutable leftCSR =
        Unchecked.defaultof<Backend.CSRMatrix<bool>>

    let mutable rightCSR =
        Unchecked.defaultof<Backend.CSRMatrix<bool>>

    let mutable resultCSR =
        Unchecked.defaultof<Backend.CSRMatrix<bool>>

    member val FirstMatrix = Unchecked.defaultof<CSRMatrix<bool>> with get, set
    member val SecondMatrix = Unchecked.defaultof<CSRMatrix<bool>> with get, set

    member this.Processor = this.OclContext.Provider.CommandQueue
    // To do: set the correct workGroupSize
    member this.eWiseAdd =
        Backend.CSRMatrix.eWiseAdd (this.OclContext) <@ (||) @> 128 this.Processor

    [<IterationCleanup>]
    member this.ClearBuffers() =
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCSR.RowPointers))
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCSR.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(leftCSR.Values))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCSR.RowPointers))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCSR.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(rightCSR.Values))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCSR.RowPointers))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCSR.Columns))
        this.Processor.Post(Msg.CreateFreeMsg<_>(resultCSR.Values))
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

                let mnMatrixCSR =
                    MathNet.Numerics.LinearAlgebra.Storage.SparseCompressedRowMatrixStorage.OfMatrix(mnMatrix.Storage)

                let mnMatrix1 = mnMatrix.Multiply(mnMatrix)

                let mnMatrix1CSR =
                    MathNet.Numerics.LinearAlgebra.Storage.SparseCompressedRowMatrixStorage.OfMatrix(mnMatrix1.Storage)

                let m =
                    { CSRMatrix.ColumnCount = mnMatrixCSR.ColumnCount
                      RowCount = mnMatrixCSR.RowCount
                      ColumnIndices = mnMatrixCSR.ColumnIndices
                      RowPointers = mnMatrixCSR.RowPointers.[..mnMatrixCSR.RowPointers.Length - 2]
                      Values = Array.map (fun _ -> true) mnMatrixCSR.Values }

                let m1 =
                    { CSRMatrix.ColumnCount = mnMatrix1CSR.ColumnCount
                      RowCount = mnMatrix1CSR.RowCount
                      ColumnIndices = mnMatrix1CSR.ColumnIndices
                      RowPointers = mnMatrix1CSR.RowPointers.[..mnMatrix1CSR.RowPointers.Length - 2]
                      Values = Array.map (fun _ -> true) mnMatrix1CSR.Values }

                m, m1
            | _ -> failwith "Unsupported matrix format"

        this.FirstMatrix <- matrix
        this.SecondMatrix <- matrix1

    [<IterationSetup>]
    member this.BuildCSR() =
        let leftRowPointers =
            this.OclContext.CreateClArray this.FirstMatrix.RowPointers

        let leftCols =
            this.OclContext.CreateClArray this.FirstMatrix.ColumnIndices

        let leftVals =
            this.OclContext.CreateClArray this.FirstMatrix.Values

        let left =
            { Backend.CSRMatrix.RowCount = this.FirstMatrix.RowCount
              Backend.CSRMatrix.ColumnCount = this.FirstMatrix.ColumnCount
              Backend.CSRMatrix.RowPointers = leftRowPointers
              Backend.CSRMatrix.Columns = leftCols
              Backend.CSRMatrix.Values = leftVals }

        leftCSR <- left

        let rightRowPointers =
            this.OclContext.CreateClArray this.SecondMatrix.RowPointers

        let rightCols =
            this.OclContext.CreateClArray this.SecondMatrix.ColumnIndices

        let rightVals =
            this.OclContext.CreateClArray this.SecondMatrix.Values

        let right =
            { Backend.CSRMatrix.RowCount = this.SecondMatrix.RowCount
              Backend.CSRMatrix.ColumnCount = this.SecondMatrix.ColumnCount
              Backend.CSRMatrix.RowPointers = rightRowPointers
              Backend.CSRMatrix.Columns = rightCols
              Backend.CSRMatrix.Values = rightVals }

        rightCSR <- right

    [<Benchmark>]
    member this.EWiseAdditionCSRBool() =
        resultCSR <- this.eWiseAdd leftCSR rightCSR

    static member InputMatricesProvider =
        "EWiseAddBenchmarks4BoolCSR.txt"
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix "EWiseAdd" matrixFilename)
                | _ -> failwith "Unsupported matrix format")
