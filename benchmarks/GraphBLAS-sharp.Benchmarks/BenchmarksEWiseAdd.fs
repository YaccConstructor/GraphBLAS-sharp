namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp.MatrixBackend
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Engines
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open BenchmarkDotNet.Filters
open BenchmarkDotNet.Jobs
open BenchmarkDotNet.Order
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open OpenCL.Net

type Config() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", fun matrix -> matrix.MatrixStructure.RowCount) :> IColumn,
            MatrixShapeColumn("ColumnCount", fun matrix -> matrix.MatrixStructure.ColumnCount) :> IColumn,
            MatrixShapeColumn("NNZ", fun matrix -> matrix.MatrixStructure.Values.Length) :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        ) |> ignore

        base.AddFilter(
            DisjunctionFilter(
                NameFilter(fun name -> name.Contains "COO") :> IFilter
            )
        ) |> ignore

[<Config(typeof<Config>)>]
[<SimpleJob(RunStrategy.Monitoring, targetCount=2)>]
type EWiseAddBenchmarks() =
    member val FirstMatrix = Unchecked.defaultof<COOFormat<float32>> with get, set
    member val SecondMatrix = Unchecked.defaultof<COOFormat<float32>> with get, set

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrix = Unchecked.defaultof<InputMatrixFormat> with get, set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContext = Unchecked.defaultof<ClContext> with get, set

    [<GlobalSetup>]
    member this.FormInputData() =
        let transposeCOO (matrix: COOFormat<float32>) =
            (matrix.Rows, matrix.Columns, matrix.Values)
            |||> Array.zip3
            |> Array.sortBy (fun (row, col, value) -> row)
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

        this.FirstMatrix <- this.InputMatrix.MatrixStructure
        this.SecondMatrix <- this.InputMatrix.MatrixStructure |> transposeCOO

    [<IterationCleanup>]
    member this.ClearBuffers() =
        let (ClContext context) = this.OclContext
        context.Provider.CloseAllBuffers()

    /// Sequence of paths to files where data for benchmarking will be taken from
    static member InputMatricesProvider =
        let matricesFilenames =
            seq {
                "arc130.mtx"
                "linux_call_graph.mtx"
                "webbase-1M.mtx"
            }

        matricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                let getFullPathToMatrix filename =
                    Path.Combine [|
                        __SOURCE_DIRECTORY__
                        "Datasets"
                        "EWiseAddDatasets"
                        filename
                    |]

                let fullPath = getFullPathToMatrix matrixFilename
                let matrixName = Path.GetFileNameWithoutExtension matrixFilename
                let matrixStructure = GraphReader.readMtx fullPath
                {
                    MatrixName = matrixName
                    MatrixStructure = matrixStructure
                }
            )

    static member AvaliableContexts =
        let mutable e = ErrorCode.Unknown
        Cl.GetPlatformIDs &e
        |> Array.collect (fun platform -> Cl.GetDeviceIDs(platform, DeviceType.All, &e))
        |> Seq.ofArray
        |> Seq.map
            (fun device ->
                let platform = Cl.GetDeviceInfo(device, DeviceInfo.Platform, &e).CastTo<Platform>()
                let platformName = Cl.GetPlatformInfo(platform, PlatformInfo.Name, &e).ToString()
                let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()
                OpenCLEvaluationContext(platformName, deviceType) |> ClContext
            )

type EWiseAddBenchmarks4Float32() =
    inherit EWiseAddBenchmarks()

    let mutable leftCOO = Unchecked.defaultof<Matrix<float32>>
    let mutable rightCOO = Unchecked.defaultof<Matrix<float32>>

    [<IterationSetup>]
    member this.BuildCOO() =
        let leftRows = Array.zeroCreate<int> base.FirstMatrix.Rows.Length
        let leftCols = Array.zeroCreate<int> base.FirstMatrix.Columns.Length
        let leftVals = Array.zeroCreate<float32> base.FirstMatrix.Values.Length
        Array.blit base.FirstMatrix.Rows 0 leftRows 0 base.FirstMatrix.Rows.Length
        Array.blit base.FirstMatrix.Columns 0 leftCols 0 base.FirstMatrix.Columns.Length
        Array.blit base.FirstMatrix.Values 0 leftVals 0 base.FirstMatrix.Values.Length

        leftCOO <-
            Matrix.Build<float32>(
                base.FirstMatrix.RowCount,
                base.FirstMatrix.ColumnCount,
                leftRows,
                leftCols,
                leftVals,
                COO
            )

        let rightRows = Array.zeroCreate<int> base.SecondMatrix.Rows.Length
        let rightCols = Array.zeroCreate<int> base.SecondMatrix.Columns.Length
        let rightVals = Array.zeroCreate<float32> base.SecondMatrix.Values.Length
        Array.blit base.SecondMatrix.Rows 0 rightRows 0 base.SecondMatrix.Rows.Length
        Array.blit base.SecondMatrix.Columns 0 rightCols 0 base.SecondMatrix.Columns.Length
        Array.blit base.SecondMatrix.Values 0 rightVals 0 base.SecondMatrix.Values.Length

        rightCOO <-
            Matrix.Build<float32>(
                base.SecondMatrix.RowCount,
                base.SecondMatrix.ColumnCount,
                rightRows,
                rightCols,
                rightVals,
                COO
            )

    [<Benchmark>]
    member this.EWiseAdditionCOOFloat32() =
        let (ClContext context) = this.OclContext
        leftCOO.EWiseAdd rightCOO None Float32Semiring.addMult
        |> context.RunSync

type EWiseAddBenchmarks4Bool() =
    inherit EWiseAddBenchmarks()

    let mutable leftCOO = Unchecked.defaultof<Matrix<bool>>
    let mutable rightCOO = Unchecked.defaultof<Matrix<bool>>

    [<IterationSetup>]
    member this.BuildCOO() =
        let leftRows = Array.zeroCreate<int> base.FirstMatrix.Rows.Length
        let leftCols = Array.zeroCreate<int> base.FirstMatrix.Columns.Length
        let leftVals = Array.create<bool> base.FirstMatrix.Values.Length true
        Array.blit base.FirstMatrix.Rows 0 leftRows 0 base.FirstMatrix.Rows.Length
        Array.blit base.FirstMatrix.Columns 0 leftCols 0 base.FirstMatrix.Columns.Length

        leftCOO <-
            Matrix.Build<bool>(
                base.FirstMatrix.RowCount,
                base.FirstMatrix.ColumnCount,
                leftRows,
                leftCols,
                leftVals,
                COO
            )

        let rightRows = Array.zeroCreate<int> base.SecondMatrix.Rows.Length
        let rightCols = Array.zeroCreate<int> base.SecondMatrix.Columns.Length
        let rightVals = Array.create<bool> base.SecondMatrix.Values.Length true
        Array.blit base.SecondMatrix.Rows 0 rightRows 0 base.SecondMatrix.Rows.Length
        Array.blit base.SecondMatrix.Columns 0 rightCols 0 base.SecondMatrix.Columns.Length

        rightCOO <-
            Matrix.Build<bool>(
                base.SecondMatrix.RowCount,
                base.SecondMatrix.ColumnCount,
                rightRows,
                rightCols,
                rightVals,
                COO
            )

    [<Benchmark>]
    member this.EWiseAdditionCOOBool() =
        let (ClContext context) = this.OclContext
        leftCOO.EWiseAdd rightCOO None BooleanSemiring.anyAll
        |> context.RunSync
