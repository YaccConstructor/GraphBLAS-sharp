namespace rec GraphBLAS.FSharp.Benchmarks

open BenchmarkDotNet.Columns
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Running
open Brahma.FSharp
open Brahma.FSharp.OpenCL.Translator
open OpenCL.Net
open GraphBLAS.FSharp.IO
open System.IO
open System.Text.RegularExpressions
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Jobs
open GraphBLAS.FSharp.Tests
open FsCheck
open Expecto
open GraphBLAS.FSharp.Test

type CommonConfig() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn<MtxReader * MtxReader>("RowCount", (fun (mtxReader, _) -> mtxReader.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixShapeColumn<MtxReader * MtxReader>("ColumnCount", (fun (mtxReader, _) -> mtxReader.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixShapeColumn<MtxReader * MtxReader>("NNZ", (fun (mtxReader, _) -> mtxReader.ReadMatrixShape().Nnz)) :> IColumn,
            MatrixShapeColumn<MtxReader * MtxReader>("SqrNNZ", (fun (_, mtxReader) -> mtxReader.ReadMatrixShape().Nnz)) :> IColumn,
            TEPSColumn(fun (parameters: obj) -> parameters :?> MtxReader * MtxReader |> fst) :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

        base.AddJob(
            Job
                .Dry
                .WithWarmupCount(3)
                .WithIterationCount(10)
                .WithInvocationCount(3)
        )
        |> ignore

type AlgorithmConfig() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn<MtxReader>("RowCount", (fun (mtxReader) -> mtxReader.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixShapeColumn<MtxReader>("ColumnCount", (fun (mtxReader) -> mtxReader.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixShapeColumn<MtxReader>("NNZ", (fun (mtxReader) -> mtxReader.ReadMatrixShape().Nnz)) :> IColumn,
            TEPSColumn(fun (parameters: obj) -> parameters :?> MtxReader) :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

        base.AddJob(
            Job
                .Dry
                .WithWarmupCount(3)
                .WithIterationCount(10)
                .WithInvocationCount(3)
        )
        |> ignore

type MatrixShapeColumn<'shape>(columnName: string, getShape: 'shape -> int) =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Params
        member this.ColumnName: string = columnName

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase) : string =
            let inputMatrix =
                benchmarkCase.Parameters.["InputMatrixReader"] :?> 'shape

            sprintf "%i" <| getShape inputMatrix

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase, style: SummaryStyle) : string =
            (this :> IColumn).GetValue(summary, benchmarkCase)

        member this.Id: string =
            sprintf "%s.%s" "MatrixShapeColumn" columnName

        member this.IsAvailable(summary: Summary) : bool = true
        member this.IsDefault(summary: Summary, benchmarkCase: BenchmarkCase) : bool = false
        member this.IsNumeric: bool = true
        member this.Legend: string = sprintf "%s of input matrix" columnName
        member this.PriorityInCategory: int = 1
        member this.UnitType: UnitType = UnitType.Size

type TEPSColumn(getMtxReader: obj -> MtxReader) =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Statistics
        member this.ColumnName: string = "TEPS"

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase) : string =
            let inputMatrixReader = getMtxReader benchmarkCase.Parameters.["InputMatrixReader"]

            let matrixShape = inputMatrixReader.ReadMatrixShape()

            let (nrows, ncols) =
                matrixShape.RowCount, matrixShape.ColumnCount

            let (vertices, edges) =
                match inputMatrixReader.Format with
                | Coordinate ->
                    if nrows = ncols then
                        (nrows, matrixShape.Nnz)
                    else
                        (ncols, nrows)
                | _ -> failwith "Unsupported"

            if isNull summary.[benchmarkCase].ResultStatistics then
                "NA"
            else
                let meanTime =
                    summary.[benchmarkCase].ResultStatistics.Mean

                sprintf "%f" <| float edges / (meanTime * 1e-6)

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase, style: SummaryStyle) : string =
            (this :> IColumn).GetValue(summary, benchmarkCase)

        member this.Id: string = "TEPSColumn"
        member this.IsAvailable(summary: Summary) : bool = true
        member this.IsDefault(summary: Summary, benchmarkCase: BenchmarkCase) : bool = false
        member this.IsNumeric: bool = true
        member this.Legend: string = "Traversed edges per second"
        member this.PriorityInCategory: int = 0
        member this.UnitType: UnitType = UnitType.Dimensionless

module Utils =
    type BenchmarkContext =
        { ClContext: Brahma.FSharp.ClContext
          Queue: MailboxProcessor<Msg> }

    let getMatricesFilenames configFilename =
        let getFullPathToConfig filename =
            Path.Combine [| __SOURCE_DIRECTORY__
                            "Configs"
                            filename |]
            |> Path.GetFullPath


        configFilename
        |> getFullPathToConfig
        |> File.ReadLines
        |> Seq.filter (fun line -> not <| line.StartsWith "!")

    let getFullPathToMatrix datasetsFolder matrixFilename =
        Path.Combine [| __SOURCE_DIRECTORY__
                        "Datasets"
                        datasetsFolder
                        matrixFilename |]

    let avaliableContexts =
        let pathToConfig =
            Path.Combine [| __SOURCE_DIRECTORY__
                            "Configs"
                            "Context.txt" |]
            |> Path.GetFullPath

        use reader = new StreamReader(pathToConfig)
        let platformRegex = Regex <| reader.ReadLine()

        let deviceType =
            match reader.ReadLine() with
            | "Cpu" -> DeviceType.Cpu
            | "Gpu" -> DeviceType.Gpu
            | "Default" -> DeviceType.Default
            | _ -> failwith "Unsupported"

        let workGroupSizes =
            reader.ReadLine()
            |> (fun s -> s.Split ' ')
            |> Seq.map int

        let mutable e = ErrorCode.Unknown

        let contexts =
            Cl.GetPlatformIDs &e
            |> Array.collect (fun platform -> Cl.GetDeviceIDs(platform, deviceType, &e))
            |> Seq.ofArray
            |> Seq.distinctBy
                (fun device ->
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Name, &e)
                        .ToString())
            |> Seq.filter
                (fun device ->
                    let platform =
                        Cl
                            .GetDeviceInfo(device, DeviceInfo.Platform, &e)
                            .CastTo<Platform>()

                    let platformName =
                        Cl
                            .GetPlatformInfo(platform, PlatformInfo.Name, &e)
                            .ToString()

                    platformRegex.IsMatch platformName)
            |> Seq.map
                (fun device ->
                    let platform =
                        Cl
                            .GetDeviceInfo(device, DeviceInfo.Platform, &e)
                            .CastTo<Platform>()

                    let clPlatform =
                        Cl
                            .GetPlatformInfo(platform, PlatformInfo.Name, &e)
                            .ToString()
                        |> Platform.Custom

                    let deviceType =
                        Cl
                            .GetDeviceInfo(device, DeviceInfo.Type, &e)
                            .CastTo<DeviceType>()

                    let clDeviceType =
                        match deviceType with
                        | DeviceType.Cpu -> ClDeviceType.Cpu
                        | DeviceType.Gpu -> ClDeviceType.Gpu
                        | DeviceType.Default -> ClDeviceType.Default
                        | _ -> failwith "Unsupported"

                    let device =
                        ClDevice.GetFirstAppropriateDevice(clPlatform)

                    let translator = FSQuotationToOpenCLTranslator device

                    let context =
                        Brahma.FSharp.ClContext(device, translator)

                    let queue = context.QueueProvider.CreateQueue()

                    { ClContext = context; Queue = queue })

        seq {
            for wgSize in workGroupSizes do
                for context in contexts do
                    yield (context, wgSize)
        }

    let nextSingle (random: System.Random) =
        let buffer = Array.zeroCreate<byte> 4
        random.NextBytes buffer
        System.BitConverter.ToSingle(buffer, 0)

    let nextInt (random: System.Random) =
        random.Next()

module VectorGenerator =
    let private pairOfVectorsOfEqualSize (valuesGenerator: Gen<'a>) createVector =
        gen {
            let! length = Gen.sized <| Gen.constant

            let! leftArray = Gen.arrayOfLength length valuesGenerator

            let! rightArray = Gen.arrayOfLength length valuesGenerator

            return (createVector leftArray, createVector rightArray)
        }

    let intPair format =
        fun array -> Utils.createVectorFromArray format array ((=) 0)
        |> pairOfVectorsOfEqualSize Arb.generate<int32>

    let floatPair format =
        let normalFloatGenerator =
            (Arb.Default.NormalFloat()
            |> Arb.toGen
            |> Gen.map float)

        let fIsEqual x y = abs (x - y) < Accuracy.medium.absolute || x = y

        let createVector array = Utils.createVectorFromArray format array (fIsEqual 0.0)

        pairOfVectorsOfEqualSize normalFloatGenerator createVector

module MatrixGenerator =
    let private pairOfMatricesOfEqualSizeGenerator (valuesGenerator: Gen<'a>) createMatrix =
        gen {
            let! nrows, ncols = Generators.dimension2DGenerator
            let! matrixA = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
            let! matrixB = valuesGenerator |> Gen.array2DOfDim (nrows, ncols)
            return (createMatrix matrixA, createMatrix matrixB)
        }

    let intPairOfEqualSizes format =
        fun array -> Utils.createMatrixFromArray2D format array ((=) 0)
        |> pairOfMatricesOfEqualSizeGenerator Arb.generate<int32>

    let floatPairOfEqualSizes format =
        let normalFloatGenerator =
            (Arb.Default.NormalFloat()
            |> Arb.toGen
            |> Gen.map float)

        let fIsEqual x y = abs (x - y) < Accuracy.medium.absolute || x = y

        fun array -> Utils.createMatrixFromArray2D format array (fIsEqual 0.0)
        |> pairOfMatricesOfEqualSizeGenerator normalFloatGenerator
