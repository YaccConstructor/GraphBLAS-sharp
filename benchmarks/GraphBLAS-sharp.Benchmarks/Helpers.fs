namespace rec GraphBLAS.FSharp.Benchmarks

open BenchmarkDotNet.Columns
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Running
open Brahma.FSharp.OpenCL
open OpenCL.Net
open GraphBLAS.FSharp.IO
open System.IO
open System.Text.RegularExpressions
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Jobs
open GraphBLAS.FSharp

type CommonConfig() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", (fun mtxReader -> mtxReader.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixShapeColumn("ColumnCount", (fun mtxReader -> mtxReader.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixShapeColumn("NNZ", (fun mtxReader -> mtxReader.ReadMatrixShape().Nnz)) :> IColumn,
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

type ClContext =
    | ClContext of Brahma.FSharp.OpenCL.ClContext
    override this.ToString() =
        let mutable e = ErrorCode.Unknown
        let (ClContext context) = this
        let device = context.Device

        let deviceName =
            Cl
                .GetDeviceInfo(device, DeviceInfo.Name, &e)
                .ToString()

        if deviceName.Length < 20 then
            sprintf "%s" deviceName
        else
            let platform =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Platform, &e)
                    .CastTo<Platform>()

            let platformName =
                Cl
                    .GetPlatformInfo(platform, PlatformInfo.Name, &e)
                    .ToString()

            let deviceType =
                match Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>() with
                | DeviceType.Cpu -> "CPU"
                | DeviceType.Gpu -> "GPU"
                | DeviceType.Accelerator -> "Accelerator"
                | _ -> "another"

            sprintf "%s, %s" platformName deviceType

type MatrixShapeColumn(columnName: string, getShape: MtxReader -> int) =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Params
        member this.ColumnName: string = columnName

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase) : string =
            let inputMatrix =
                benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader

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
                benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader

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

        let mutable e = ErrorCode.Unknown

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
                    |> ClPlatform.Custom

                let deviceType =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Type, &e)
                        .CastTo<DeviceType>()

                let clDeviceType =
                    match deviceType with
                    | DeviceType.Cpu -> ClDeviceType.CPU
                    | DeviceType.Gpu -> ClDeviceType.GPU
                    | DeviceType.Default -> ClDeviceType.Default
                    | _ -> failwith "Unsupported"

                Brahma.FSharp.OpenCL.ClContext(clPlatform, clDeviceType))

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

module EWiseAdd =
    //After matrix generalization this functions can be merged into one
    let getBuffersOfMatrixCSR (matrix: Backend.CSRMatrix<'a>) =
        matrix.RowPointers, matrix.Columns, matrix.Values

    let getBuffersOfMatrixCOO (matrix: Backend.COOMatrix<'a>) =
        matrix.Rows, matrix.Columns, matrix.Values

    let buffersCleanup (processor: MailboxProcessor<_>) getBuffersFun matrix =
        let buffers = getBuffersFun matrix

        match buffers with
        | b1, b2, b3 ->
            processor.Post(Msg.CreateFreeMsg<_>(b1))
            processor.Post(Msg.CreateFreeMsg<_>(b2))
            processor.Post(Msg.CreateFreeMsg<_>(b3))

        processor.PostAndReply(Msg.MsgNotifyMe)

    let readMatrixAndSquaredCSR
        (context: Brahma.FSharp.OpenCL.ClContext)
        (reader: MtxReader)
        datasetFolder
        converter
        converterBool
        toCSR
        =
        let squaredMatrixReader =
            MtxReader(Utils.getFullPathToMatrix datasetFolder ("squared_" + reader.ToString()))

        let matrixWrapped, matrixWrapped1 =
            match reader.Field, squaredMatrixReader.Field with
            | Pattern, Pattern ->
                reader.ReadMatrixBoolean(converterBool), squaredMatrixReader.ReadMatrixBoolean(converterBool)
            | Pattern, _ -> reader.ReadMatrixBoolean(converterBool), squaredMatrixReader.ReadMatrix(converter)
            | _, Pattern -> reader.ReadMatrix(converter), squaredMatrixReader.ReadMatrixBoolean(converterBool)
            | _, _ -> reader.ReadMatrix(converter), squaredMatrixReader.ReadMatrix(converter)

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
