namespace GraphBLAS.FSharp.Benchmarks

open GraphBLAS.FSharp.Algorithms
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Running

type TEPSColumn() =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Statistics
        member this.ColumnName: string = "TEPS"
        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase): string =
            // let a = summary.[benchmarkCase] .ResultStatistics.Mean
            "?????????????"
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
