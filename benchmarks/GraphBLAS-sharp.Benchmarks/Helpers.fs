namespace GraphBLAS.FSharp.Benchmarks

open BenchmarkDotNet.Columns
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Running
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open OpenCL.Net
open GraphBLAS.FSharp.IO
open System.IO

type ClContext = ClContext of OpenCLEvaluationContext
with
    override this.ToString() =
        let mutable e = ErrorCode.Unknown
        let (ClContext context) = this
        let device = context.Device
        let deviceName = Cl.GetDeviceInfo(device, DeviceInfo.Name, &e).ToString()
        if deviceName.Length < 20 then
            sprintf "%s" deviceName
        else
            let platform = Cl.GetDeviceInfo(device, DeviceInfo.Platform, &e).CastTo<Platform>()
            let platformName = Cl.GetPlatformInfo(platform, PlatformInfo.Name, &e).ToString()
            let deviceType =
                match Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>() with
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

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase): string =
            let inputMatrix = benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader
            sprintf "%i" <| getShape inputMatrix

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase, style: SummaryStyle): string =
            (this :> IColumn).GetValue(summary, benchmarkCase)

        member this.Id: string = sprintf "%s.%s" "MatrixShapeColumn" columnName
        member this.IsAvailable(summary: Summary): bool = true
        member this.IsDefault(summary: Summary, benchmarkCase: BenchmarkCase): bool = false
        member this.IsNumeric: bool = true
        member this.Legend: string = sprintf "%s of input matrix" columnName
        member this.PriorityInCategory: int = 1
        member this.UnitType: UnitType = UnitType.Size

type TEPSColumn() =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Statistics
        member this.ColumnName: string = "TEPS"

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase): string =
            let inputMatrixReader = benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader
            let matrixShape = inputMatrixReader.ReadMatrixShape()

            let (nrows, ncols) = matrixShape.RowCount, matrixShape.ColumnCount
            let (vertices, edges) =
                match inputMatrixReader.Format with
                | Coordinate -> if nrows = ncols then (nrows, matrixShape.Nnz) else (ncols, nrows)
                | _ -> failwith "Unsupported"

            if isNull summary.[benchmarkCase].ResultStatistics then
                "NA"
            else
                let meanTime = summary.[benchmarkCase].ResultStatistics.Mean
                sprintf "%f" <| float edges / (meanTime * 1e-6)

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase, style: SummaryStyle): string =
            (this :> IColumn).GetValue(summary, benchmarkCase)

        member this.Id: string = "TEPSColumn"
        member this.IsAvailable(summary: Summary): bool = true
        member this.IsDefault(summary: Summary, benchmarkCase: BenchmarkCase): bool = false
        member this.IsNumeric: bool = true
        member this.Legend: string = "Traversed edges per second"
        member this.PriorityInCategory: int = 0
        member this.UnitType: UnitType = UnitType.Dimensionless

module Utils =
    let getMatricesFilenames configFilename =
        let getFullPathToConfig filename =
            Path.Combine [|
                __SOURCE_DIRECTORY__
                "Configs"
                filename
            |] |> Path.GetFullPath

        configFilename
        |> getFullPathToConfig
        |> File.ReadLines
        |> Seq.filter (fun line -> not <| line.StartsWith "!")

    let getFullPathToMatrix datasetsFolder matrixFilename =
        Path.Combine [|
            __SOURCE_DIRECTORY__
            "Datasets"
            datasetsFolder
            matrixFilename
        |]
