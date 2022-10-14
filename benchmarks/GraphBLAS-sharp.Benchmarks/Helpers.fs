namespace rec GraphBLAS.FSharp.Benchmarks

open BenchmarkDotNet.Columns
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Running
open Brahma.FSharp
open Brahma.FSharp.OpenCL.Shared
open Brahma.FSharp.OpenCL.Translator
open OpenCL.Net
open GraphBLAS.FSharp.IO
open System.IO
open System.Text.RegularExpressions
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Jobs
open GraphBLAS.FSharp

open System.IO
open GraphBLAS.FSharp
open System

type CommonConfig() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", (fun (mtxReader, _) -> mtxReader.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixShapeColumn("ColumnCount", (fun (mtxReader, _) -> mtxReader.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixShapeColumn("NNZ", (fun (mtxReader, _) -> mtxReader.ReadMatrixShape().Nnz)) :> IColumn,
            MatrixShapeColumn("SqrNNZ", (fun (_, mtxReader) -> mtxReader.ReadMatrixShape().Nnz)) :> IColumn,
            TEPSColumn() :> IColumn,
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

type MatrixShapeColumn(columnName: string, getShape: (MtxReader * MtxReader) -> int) =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Params
        member this.ColumnName: string = columnName

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase) : string =
            let inputMatrix =
                benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader * MtxReader

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

type TEPSColumn() =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Statistics
        member this.ColumnName: string = "TEPS"

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase) : string =
            let inputMatrixReader =
                benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader * MtxReader
                |> fst

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

    let rowPointers2rowIndices (rowPointers: int []) =
        let rowIndices =
            Array.zeroCreate rowPointers.[rowPointers.Length - 1]

        [| 0 .. rowPointers.Length - 2 |]
        |> Array.Parallel.iter
            (fun i ->
                [| rowPointers.[i] .. rowPointers.[i + 1] - 1 |]
                |> Array.Parallel.iter (fun j -> rowIndices.[j] <- i))

        rowIndices

    let rowIndices2rowPointers (rowIndices: int []) rowCount =
        let nnzPerRow = Array.zeroCreate rowCount
        let rowPointers = Array.zeroCreate rowCount

        Array.iter (fun rowIndex -> nnzPerRow.[rowIndex] <- nnzPerRow.[rowIndex] + 1) rowIndices

        for i in 1 .. rowCount - 1 do
            rowPointers.[i] <- rowPointers.[i - 1] + nnzPerRow.[i - 1]

        rowPointers

    // let csr2csc (rowPointers: int []) (columnIndices: int []) columnCount =
    //     let rowIndices = rowPointers2rowIndices rowPointers
    //     let rowIndices, columnIndices =
    //         (rowIndices, columnIndices)
    //         ||> Array.zip
    //         |> Array.sortBy snd
    //         |> Array.unzip
    //     let columnPointers = rowIndices2rowPointers columnIndices columnCount
    //     rowIndices, columnPointers

    let inline buildCooMatrix (context:ClContext) matrix =
        match matrix with
        | MatrixCOO m ->
            let rows =
                context.CreateClArray (m.Rows, hostAccessMode = HostAccessMode.ReadOnly, deviceAccessMode = DeviceAccessMode.ReadOnly, allocationMode = AllocationMode.CopyHostPtr)

            let cols =
                context.CreateClArray (m.Columns, hostAccessMode = HostAccessMode.ReadOnly, deviceAccessMode = DeviceAccessMode.ReadOnly, allocationMode = AllocationMode.CopyHostPtr)

            let vals =
                context.CreateClArray (m.Values, hostAccessMode = HostAccessMode.ReadOnly, deviceAccessMode = DeviceAccessMode.ReadOnly, allocationMode = AllocationMode.CopyHostPtr)

            { Backend.COOMatrix.Context = context
              Backend.COOMatrix.RowCount = m.RowCount
              Backend.COOMatrix.ColumnCount = m.ColumnCount
              Backend.COOMatrix.Rows = rows
              Backend.COOMatrix.Columns = cols
              Backend.COOMatrix.Values = vals }

        | x -> failwith "Unsupported matrix format: %A"

    let inline buildCsrMatrix (context:ClContext) matrix =
        match matrix with
        | MatrixCOO m ->
            let rowPointers =
                context.CreateClArray(
                    Utils.rowIndices2rowPointers m.Rows m.RowCount
                    ,hostAccessMode = HostAccessMode.ReadOnly
                    ,deviceAccessMode = DeviceAccessMode.ReadOnly
                    ,allocationMode = AllocationMode.CopyHostPtr)

            let cols =
                context.CreateClArray (
                    m.Columns
                    ,hostAccessMode = HostAccessMode.ReadOnly
                    ,deviceAccessMode = DeviceAccessMode.ReadOnly
                    ,allocationMode = AllocationMode.CopyHostPtr)

            let vals =
                context.CreateClArray (
                    m.Values
                    ,hostAccessMode = HostAccessMode.ReadOnly
                    ,deviceAccessMode = DeviceAccessMode.ReadOnly
                    ,allocationMode = AllocationMode.CopyHostPtr)

            { Backend.CSRMatrix.Context = context
              Backend.CSRMatrix.RowCount = m.RowCount
              Backend.CSRMatrix.ColumnCount = m.ColumnCount
              Backend.CSRMatrix.RowPointers = rowPointers
              Backend.CSRMatrix.Columns = cols
              Backend.CSRMatrix.Values = vals }

        | x -> failwith "Unsupported matrix format: %A"
