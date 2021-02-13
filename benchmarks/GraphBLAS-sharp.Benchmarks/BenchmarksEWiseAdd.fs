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
open Brahma.OpenCL
open System.Text.RegularExpressions

type Config() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", fun matrix -> matrix.RowCount) :> IColumn,
            MatrixShapeColumn("ColumnCount", fun matrix -> matrix.ColumnCount) :> IColumn,
            MatrixShapeColumn("NNZ", fun matrix ->
                match matrix.Format with
                | "coordinate" -> matrix.Size.[2]
                | "array" -> 0
                | _ -> failwith "Unsupported") :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        ) |> ignore

        base.AddFilter(
            DisjunctionFilter(
                NameFilter(fun name -> name.Contains "COO") :> IFilter
            )
        ) |> ignore

[<IterationCount(1)>]
[<WarmupCount(1)>]
[<Config(typeof<Config>)>]
type EWiseAddBenchmarks() =
    [<ParamsSource("AvaliableContexts")>]
    member val OclContext = Unchecked.defaultof<ClContext> with get, set

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrix = Unchecked.defaultof<MtxShape> with get, set

    [<IterationCleanup>]
    member this.ClearBuffers() =
        let (ClContext context) = this.OclContext
        context.Provider.CloseAllBuffers()

    [<GlobalCleanup>]
    member this.ClearContext() =
        let (ClContext context) = this.OclContext
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

type EWiseAddBenchmarks4Float32() =
    inherit EWiseAddBenchmarks()

    let mutable leftCOO = Unchecked.defaultof<Matrix<float32>>
    let mutable rightCOO = Unchecked.defaultof<Matrix<float32>>

    member val FirstMatrix = Unchecked.defaultof<COOFormat<float32>> with get, set
    member val SecondMatrix = Unchecked.defaultof<COOFormat<float32>> with get, set

    [<GlobalSetup>]
    member this.FormInputData() =
        let mtxFormat = GraphReader.readMtx <| Utils.getFullPathToMatrix this.InputMatrix.Filename
        let cooMatrix =
            match mtxFormat.Shape.Format, mtxFormat.Shape.Field with
            | "coordinate", "real" -> Utils.makeCOO mtxFormat <| FromString float32
            | "coordinate", "integer" -> Utils.makeCOO mtxFormat <| FromString float32
            | "coordinate", "pattern" ->
                let rand = System.Random()
                let nextSingle (random: System.Random) =
                    let buffer = Array.zeroCreate<byte> 4
                    random.NextBytes buffer
                    System.BitConverter.ToSingle(buffer, 0)

                Utils.makeCOO mtxFormat <| FromUnit (fun () -> nextSingle rand)
            | _ -> failwith "Unsupported matrix format"

        this.FirstMatrix <- cooMatrix
        this.SecondMatrix <- cooMatrix |> Utils.transposeCOO

    [<IterationSetup>]
    member this.BuildCOO() =
        let leftRows = Array.zeroCreate<int> this.FirstMatrix.Rows.Length
        let leftCols = Array.zeroCreate<int> this.FirstMatrix.Columns.Length
        let leftVals = Array.zeroCreate<float32> this.FirstMatrix.Values.Length
        Array.blit this.FirstMatrix.Rows 0 leftRows 0 this.FirstMatrix.Rows.Length
        Array.blit this.FirstMatrix.Columns 0 leftCols 0 this.FirstMatrix.Columns.Length
        Array.blit this.FirstMatrix.Values 0 leftVals 0 this.FirstMatrix.Values.Length

        leftCOO <-
            Matrix.Build<float32>(
                this.FirstMatrix.RowCount,
                this.FirstMatrix.ColumnCount,
                leftRows,
                leftCols,
                leftVals,
                COO
            )

        let rightRows = Array.zeroCreate<int> this.SecondMatrix.Rows.Length
        let rightCols = Array.zeroCreate<int> this.SecondMatrix.Columns.Length
        let rightVals = Array.zeroCreate<float32> this.SecondMatrix.Values.Length
        Array.blit this.SecondMatrix.Rows 0 rightRows 0 this.SecondMatrix.Rows.Length
        Array.blit this.SecondMatrix.Columns 0 rightCols 0 this.SecondMatrix.Columns.Length
        Array.blit this.SecondMatrix.Values 0 rightVals 0 this.SecondMatrix.Values.Length

        rightCOO <-
            Matrix.Build<float32>(
                this.SecondMatrix.RowCount,
                this.SecondMatrix.ColumnCount,
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

    static member InputMatricesProvider =
        let matricesFilenames =
            let pathToConfig =
                Path.Combine [|
                    __SOURCE_DIRECTORY__
                    "Configs"
                    "EWiseAddBenchmarks4Float32.txt"
                |] |> Path.GetFullPath

            File.ReadAllLines pathToConfig
            |> Seq.ofArray
            |> Seq.filter (fun line -> not <| line.StartsWith "!")

        matricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                match Path.GetExtension matrixFilename with
                | ".mtx" -> GraphReader.readShapeMtx <| Utils.getFullPathToMatrix matrixFilename
                | _ -> failwith "Unsupported matrix format"
            )

type EWiseAddBenchmarks4Bool() =
    inherit EWiseAddBenchmarks()

    let mutable leftCOO = Unchecked.defaultof<Matrix<bool>>
    let mutable rightCOO = Unchecked.defaultof<Matrix<bool>>

    member val FirstMatrix = Unchecked.defaultof<COOFormat<bool>> with get, set
    member val SecondMatrix = Unchecked.defaultof<COOFormat<bool>> with get, set

    [<GlobalSetup>]
    member this.FormInputData() =
        let mtxFormat = GraphReader.readMtx <| Utils.getFullPathToMatrix this.InputMatrix.Filename
        let cooMatrix =
            match mtxFormat.Shape.Format, mtxFormat.Shape.Field with
            | "coordinate", "real" -> Utils.makeCOO mtxFormat <| FromString (fun _ -> true)
            | "coordinate", "integer" -> Utils.makeCOO mtxFormat <| FromString (fun _ -> true)
            | "coordinate", "pattern" -> Utils.makeCOO mtxFormat <| FromUnit (fun _ -> true)
            | _ -> failwith "Unsupported matrix format"

        this.FirstMatrix <- cooMatrix
        this.SecondMatrix <- cooMatrix |> Utils.transposeCOO

    [<IterationSetup>]
    member this.BuildCOO() =
        let leftRows = Array.zeroCreate<int> this.FirstMatrix.Rows.Length
        let leftCols = Array.zeroCreate<int> this.FirstMatrix.Columns.Length
        let leftVals = Array.create<bool> this.FirstMatrix.Values.Length true
        Array.blit this.FirstMatrix.Rows 0 leftRows 0 this.FirstMatrix.Rows.Length
        Array.blit this.FirstMatrix.Columns 0 leftCols 0 this.FirstMatrix.Columns.Length

        leftCOO <-
            Matrix.Build<bool>(
                this.FirstMatrix.RowCount,
                this.FirstMatrix.ColumnCount,
                leftRows,
                leftCols,
                leftVals,
                COO
            )

        let rightRows = Array.zeroCreate<int> this.SecondMatrix.Rows.Length
        let rightCols = Array.zeroCreate<int> this.SecondMatrix.Columns.Length
        let rightVals = Array.create<bool> this.SecondMatrix.Values.Length true
        Array.blit this.SecondMatrix.Rows 0 rightRows 0 this.SecondMatrix.Rows.Length
        Array.blit this.SecondMatrix.Columns 0 rightCols 0 this.SecondMatrix.Columns.Length

        rightCOO <-
            Matrix.Build<bool>(
                this.SecondMatrix.RowCount,
                this.SecondMatrix.ColumnCount,
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

    static member InputMatricesProvider =
        let matricesFilenames =
            let pathToConfig =
                Path.Combine [|
                    __SOURCE_DIRECTORY__
                    "Configs"
                    "EWiseAddBenchmarks4Bool.txt"
                |] |> Path.GetFullPath

            File.ReadAllLines pathToConfig
            |> Seq.ofArray
            |> Seq.filter (fun line -> not <| line.StartsWith "!")

        matricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                match Path.GetExtension matrixFilename with
                | ".mtx" -> GraphReader.readShapeMtx <| Utils.getFullPathToMatrix matrixFilename
                | _ -> failwith "Unsupported matrix format"
            )
