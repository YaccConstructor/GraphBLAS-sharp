namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open System.Text.RegularExpressions
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open BenchmarkDotNet.Filters
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open OpenCL.Net

type Config() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", fun matrix -> matrix.ReadMatrixShape().RowCount) :> IColumn,
            MatrixShapeColumn("ColumnCount", fun matrix -> matrix.ReadMatrixShape().ColumnCount) :> IColumn,
            MatrixShapeColumn("NNZ", fun matrix ->
                match matrix.Format with
                | Coordinate -> matrix.ReadMatrixShape().Nnz
                | Array -> 0) :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        ) |> ignore

        base.AddFilter(
            DisjunctionFilter(
                NameFilter(fun name -> name.Contains "COO") :> IFilter
            )
        ) |> ignore

[<IterationCount(5)>]
[<WarmupCount(3)>]
[<Config(typeof<Config>)>]
[<CsvExporter>]
type EWiseAddBenchmarks() =
    [<ParamsSource("AvaliableContexts")>]
    member val OclContext = Unchecked.defaultof<ClContext> with get, set

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader> with get, set

    [<ParamsSource("TransferNeeds")>]
    member val IsInitialDataTransferIncluded = Unchecked.defaultof<bool> with get, set

    [<IterationCleanup>]
    member this.ClearBuffers() =
        if this.IsInitialDataTransferIncluded then
            let (ClContext context) = this.OclContext
            context.Provider.CloseAllBuffers()

    [<GlobalCleanup>]
    member this.ClearContext() =
        let (ClContext context) = this.OclContext
        if not this.IsInitialDataTransferIncluded then
            context.Provider.CloseAllBuffers()
        context.Provider.Dispose()

    static member AvaliableContexts =
        let pathToConfig =
            Path.Combine [|
                __SOURCE_DIRECTORY__
                "Configs"
                "Context.txt"
            |] |> Path.GetFullPath

        use reader = new StreamReader(pathToConfig)
        let platformRegex = Regex <| reader.ReadLine()
        let deviceType =
            match reader.ReadLine() with
            | "Cpu" -> DeviceType.Cpu
            | "Gpu" -> DeviceType.Gpu
            | "All" -> DeviceType.All
            | _ -> failwith "Unsupported"

        let mutable e = ErrorCode.Unknown
        Cl.GetPlatformIDs &e
        |> Array.collect (fun platform -> Cl.GetDeviceIDs(platform, deviceType, &e))
        |> Seq.ofArray
        |> Seq.distinctBy (fun device -> Cl.GetDeviceInfo(device, DeviceInfo.Name, &e).ToString())
        |> Seq.filter
            (fun device ->
                let platform = Cl.GetDeviceInfo(device, DeviceInfo.Platform, &e).CastTo<Platform>()
                let platformName = Cl.GetPlatformInfo(platform, PlatformInfo.Name, &e).ToString()
                platformRegex.IsMatch platformName
            )
        |> Seq.map
            (fun device ->
                let platform = Cl.GetDeviceInfo(device, DeviceInfo.Platform, &e).CastTo<Platform>()
                let platformName = Cl.GetPlatformInfo(platform, PlatformInfo.Name, &e).ToString()
                let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()
                OpenCLEvaluationContext(platformName, deviceType) |> ClContext
            )
    static member TransferNeeds = Seq.ofArray [|true; false|]

type EWiseAddBenchmarks4Float32() =
    inherit EWiseAddBenchmarks()

    let mutable leftCOO = Unchecked.defaultof<Matrix<float32>>
    let mutable rightCOO = Unchecked.defaultof<Matrix<float32>>

    member val FirstMatrix = Unchecked.defaultof<COOMatrix<float32>> with get, set
    member val SecondMatrix = Unchecked.defaultof<COOMatrix<float32>> with get, set

    [<GlobalSetup>]
    member this.FormInputData() =
        let (ClContext context) = this.OclContext
        context.IsCachingEnabled <- true

        let matrixWrapped =
            match this.InputMatrixReader.Field with
            | Real -> this.InputMatrixReader.ReadMatrixReal(float32)
            | Integer -> this.InputMatrixReader.ReadMatrixInteger(float32)
            | Pattern ->
                let rand = System.Random()
                let nextSingle (random: System.Random) =
                    let buffer = Array.zeroCreate<byte> 4
                    random.NextBytes buffer
                    System.BitConverter.ToSingle(buffer, 0)

                this.InputMatrixReader.ReadMatrixBoolean(fun _ -> nextSingle rand)
            | _ -> failwith "Unsupported matrix format"

        let matrix, matrix2 =
            match matrixWrapped with
            | MatrixCOO m ->
                let mnMatrix = MathNet.Numerics.LinearAlgebra.Matrix<float32>.Build.SparseFromCoordinateFormat(m.RowCount, m.ColumnCount, m.Values.Length, m.Rows, m.Columns, m.Values)
                let mnMatrix2 = mnMatrix.Multiply(mnMatrix)
                let storage = MathNet.Numerics.LinearAlgebra.Storage.SparseCompressedRowMatrixStorage.OfMatrix(mnMatrix2.Storage)

                let rowPointers2rowIndices (rowPointers: int[]) =
                    let rowIndices = Array.zeroCreate rowPointers.[rowPointers.Length - 1]
                    [|0 .. rowPointers.Length - 2|]
                    |> Array.Parallel.iter
                        (fun i ->
                        [|rowPointers.[i] .. rowPointers.[i + 1] - 1|]
                        |> Array.Parallel.iter (fun j -> rowIndices.[j] <- i)
                    )
                    rowIndices

                let cols = storage.ColumnIndices
                let vals = storage.Values
                let rows = rowPointers2rowIndices storage.RowPointers

                let m2 =
                    {
                        Rows = rows
                        Columns = cols
                        Values = vals
                        RowCount = m.RowCount
                        ColumnCount = m.ColumnCount
                    }

                m, m2
            | _ -> failwith "Unsupported matrix format"

        this.FirstMatrix <- matrix
        this.SecondMatrix <- matrix2

        if not this.IsInitialDataTransferIncluded then
            leftCOO <-
                COOMatrix.FromTuples(
                    this.FirstMatrix.RowCount,
                    this.FirstMatrix.ColumnCount,
                    this.FirstMatrix.Rows,
                    this.FirstMatrix.Columns,
                    this.FirstMatrix.Values
                ) |> MatrixCOO

            rightCOO <-
                COOMatrix.FromTuples(
                    this.SecondMatrix.RowCount,
                    this.SecondMatrix.ColumnCount,
                    this.SecondMatrix.Rows,
                    this.SecondMatrix.Columns,
                    this.SecondMatrix.Values
                ) |> MatrixCOO

            // Algorithm needs to be executed in order to transfer data to GPU
            let (ClContext context) = this.OclContext
            (leftCOO, rightCOO) ||> Matrix.eWiseAdd AddMult.float32
            |> EvalGB.withClContext context
            |> EvalGB.runSync
            |> ignore

    [<IterationSetup>]
    member this.BuildCOO() =
        if this.IsInitialDataTransferIncluded then
            let leftRows = Array.zeroCreate<int> this.FirstMatrix.Rows.Length
            let leftCols = Array.zeroCreate<int> this.FirstMatrix.Columns.Length
            let leftVals = Array.zeroCreate<float32> this.FirstMatrix.Values.Length
            Array.blit this.FirstMatrix.Rows 0 leftRows 0 this.FirstMatrix.Rows.Length
            Array.blit this.FirstMatrix.Columns 0 leftCols 0 this.FirstMatrix.Columns.Length
            Array.blit this.FirstMatrix.Values 0 leftVals 0 this.FirstMatrix.Values.Length

            leftCOO <-
                COOMatrix.FromTuples(
                    this.FirstMatrix.RowCount,
                    this.FirstMatrix.ColumnCount,
                    leftRows,
                    leftCols,
                    leftVals
                ) |> MatrixCOO

            let rightRows = Array.zeroCreate<int> this.SecondMatrix.Rows.Length
            let rightCols = Array.zeroCreate<int> this.SecondMatrix.Columns.Length
            let rightVals = Array.zeroCreate<float32> this.SecondMatrix.Values.Length
            Array.blit this.SecondMatrix.Rows 0 rightRows 0 this.SecondMatrix.Rows.Length
            Array.blit this.SecondMatrix.Columns 0 rightCols 0 this.SecondMatrix.Columns.Length
            Array.blit this.SecondMatrix.Values 0 rightVals 0 this.SecondMatrix.Values.Length

            rightCOO <-
                COOMatrix.FromTuples(
                    this.SecondMatrix.RowCount,
                    this.SecondMatrix.ColumnCount,
                    rightRows,
                    rightCols,
                    rightVals
                ) |> MatrixCOO

    [<Benchmark>]
    member this.EWiseAdditionCOOFloat32() =
        let (ClContext context) = this.OclContext
        (leftCOO, rightCOO) ||> Matrix.eWiseAdd AddMult.float32
        |> EvalGB.withClContext context
        |> EvalGB.runSync

    static member InputMatricesProvider =
        "EWiseAddBenchmarks4Float32.txt"
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix "EWiseAdd" matrixFilename)
                | _ -> failwith "Unsupported matrix format"
            )

type EWiseAddBenchmarks4Bool() =
    inherit EWiseAddBenchmarks()

    let mutable leftCOO = Unchecked.defaultof<Matrix<bool>>
    let mutable rightCOO = Unchecked.defaultof<Matrix<bool>>

    member val FirstMatrix = Unchecked.defaultof<COOMatrix<bool>> with get, set
    member val SecondMatrix = Unchecked.defaultof<COOMatrix<bool>> with get, set

    [<GlobalSetup>]
    member this.FormInputData() =
        let (ClContext context) = this.OclContext
        context.IsCachingEnabled <- true

        let matrixWrapped =
            match this.InputMatrixReader.Field with
            | Real -> this.InputMatrixReader.ReadMatrixReal(fun _ -> true)
            | Integer -> this.InputMatrixReader.ReadMatrixInteger(fun _ -> true)
            | Pattern -> this.InputMatrixReader.ReadMatrixBoolean(fun _ -> true)
            | _ -> failwith "Unsupported matrix format"

        let matrix, matrix2 =
            match matrixWrapped with
            | MatrixCOO m ->
                let mnMatrix = MathNet.Numerics.LinearAlgebra.Matrix<int>.Build.SparseFromCoordinateFormat(m.RowCount, m.ColumnCount, m.Values.Length,
                                m.Rows, m.Columns, Array.map (fun _ -> 1) m.Values)
                let mnMatrix2 = mnMatrix.Multiply(mnMatrix)
                let storage = MathNet.Numerics.LinearAlgebra.Storage.SparseCompressedRowMatrixStorage.OfMatrix(mnMatrix2.Storage)

                let rowPointers2rowIndices (rowPointers: int[]) =
                    let rowIndices = Array.zeroCreate rowPointers.[rowPointers.Length - 1]
                    [|0 .. rowPointers.Length - 2|]
                    |> Array.Parallel.iter
                        (fun i ->
                        [|rowPointers.[i] .. rowPointers.[i + 1] - 1|]
                        |> Array.Parallel.iter (fun j -> rowIndices.[j] <- i)
                    )
                    rowIndices

                let cols = storage.ColumnIndices
                let vals = Array.map (fun _ -> true) storage.Values
                let rows = rowPointers2rowIndices storage.RowPointers

                let m2 =
                    {
                        Rows = rows
                        Columns = cols
                        Values = vals
                        RowCount = m.RowCount
                        ColumnCount = m.ColumnCount
                    }

                m, m2
            | _ -> failwith "Unsupported matrix format"

        this.FirstMatrix <- matrix
        this.SecondMatrix <- matrix2

        if not this.IsInitialDataTransferIncluded then
            leftCOO <-
                COOMatrix.FromTuples(
                    this.FirstMatrix.RowCount,
                    this.FirstMatrix.ColumnCount,
                    this.FirstMatrix.Rows,
                    this.FirstMatrix.Columns,
                    this.FirstMatrix.Values
                ) |> MatrixCOO

            rightCOO <-
                COOMatrix.FromTuples(
                    this.SecondMatrix.RowCount,
                    this.SecondMatrix.ColumnCount,
                    this.SecondMatrix.Rows,
                    this.SecondMatrix.Columns,
                    this.SecondMatrix.Values
                ) |> MatrixCOO

            // Algorithm needs to be executed in order to transfer data to GPU
            let (ClContext context) = this.OclContext
            (leftCOO, rightCOO) ||> Matrix.eWiseAdd AnyAll.bool
            |> EvalGB.withClContext context
            |> EvalGB.runSync
            |> ignore

    [<IterationSetup>]
    member this.BuildCOO() =
        if this.IsInitialDataTransferIncluded then
            let leftRows = Array.zeroCreate<int> this.FirstMatrix.Rows.Length
            let leftCols = Array.zeroCreate<int> this.FirstMatrix.Columns.Length
            let leftVals = Array.create<bool> this.FirstMatrix.Values.Length true
            Array.blit this.FirstMatrix.Rows 0 leftRows 0 this.FirstMatrix.Rows.Length
            Array.blit this.FirstMatrix.Columns 0 leftCols 0 this.FirstMatrix.Columns.Length

            leftCOO <-
                COOMatrix.FromTuples(
                    this.FirstMatrix.RowCount,
                    this.FirstMatrix.ColumnCount,
                    leftRows,
                    leftCols,
                    leftVals
                ) |> MatrixCOO

            let rightRows = Array.zeroCreate<int> this.SecondMatrix.Rows.Length
            let rightCols = Array.zeroCreate<int> this.SecondMatrix.Columns.Length
            let rightVals = Array.create<bool> this.SecondMatrix.Values.Length true
            Array.blit this.SecondMatrix.Rows 0 rightRows 0 this.SecondMatrix.Rows.Length
            Array.blit this.SecondMatrix.Columns 0 rightCols 0 this.SecondMatrix.Columns.Length

            rightCOO <-
                COOMatrix.FromTuples(
                    this.SecondMatrix.RowCount,
                    this.SecondMatrix.ColumnCount,
                    rightRows,
                    rightCols,
                    rightVals
                ) |> MatrixCOO

    [<Benchmark>]
    member this.EWiseAdditionCOOBool() =
        let (ClContext context) = this.OclContext
        (leftCOO, rightCOO) ||> Matrix.eWiseAdd AnyAll.bool
        |> EvalGB.withClContext context
        |> EvalGB.runSync

    static member InputMatricesProvider =
        "EWiseAddBenchmarks4Bool.txt"
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix "EWiseAdd" matrixFilename)
                | _ -> failwith "Unsupported matrix format"
            )
