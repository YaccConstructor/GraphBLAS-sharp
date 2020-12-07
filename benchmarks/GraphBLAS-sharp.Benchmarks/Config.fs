namespace GraphBLAS.FSharp.Benchmarks

open GraphBLAS.FSharp.Algorithms
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Running
open System.IO

type TEPSColumn() =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Statistics
        member this.ColumnName: string = "TEPS"
        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase): string =
            let meanTime = summary.[benchmarkCase].ResultStatistics.Mean
            let pathToGraph = benchmarkCase.Parameters.["PathToGraph"].ToString()
            match Path.GetExtension pathToGraph with
            | ".mtx" ->
                use streamReader = new StreamReader(pathToGraph)
                while streamReader.Peek() = int '%' do
                    streamReader.ReadLine() |> ignore
                let matrixInfo = streamReader.ReadLine().Split(' ')
                let (nrows, ncols, nnz) = float matrixInfo.[0], float matrixInfo.[1], float matrixInfo.[2]
                let (vertices, edges) = if nrows = ncols then (nrows, nnz) else (ncols, nrows)
                sprintf "%f" (edges / meanTime)
            | _ -> "file`s format not supported"
        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase, style: SummaryStyle): string =
            (this :> IColumn).GetValue(summary, benchmarkCase)
        member this.Id: string = "TEPSColumn"
        member this.IsAvailable(summary: Summary): bool = true
        member this.IsDefault(summary: Summary, benchmarkCase: BenchmarkCase): bool = false
        member this.IsNumeric: bool = true
        member this.Legend: string = "Traversed edges per second"
        member this.PriorityInCategory: int = 0
        member this.UnitType: UnitType = UnitType.Dimensionless

type Config() =
    inherit ManualConfig()

    do
        base.AddColumn [| TEPSColumn() :> IColumn |] |> ignore
