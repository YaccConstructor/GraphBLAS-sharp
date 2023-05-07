namespace GraphBLAS.FSharp.Benchmarks.Columns

open BenchmarkDotNet.Columns
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Running
open GraphBLAS.FSharp.IO

type CommonColumn<'a>(benchmarkCaseConvert, columnName: string, getShape: 'a -> _) =
    interface IColumn with
        member this.AlwaysShow = true
        member this.Category = ColumnCategory.Params
        member this.ColumnName = columnName

        member this.GetValue(_: Summary, benchmarkCase: BenchmarkCase) =
            benchmarkCaseConvert benchmarkCase
            |> getShape
            |> sprintf "%A"

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase, _: SummaryStyle) =
            (this :> IColumn).GetValue(summary, benchmarkCase)

        member this.Id = sprintf $"%s{columnName}"

        member this.IsAvailable(_: Summary) = true
        member this.IsDefault(_: Summary, _: BenchmarkCase) = false
        member this.IsNumeric = true
        member this.Legend = sprintf $"%s{columnName}"
        member this.PriorityInCategory = 1
        member this.UnitType = UnitType.Size

type MatrixColumn(name, getShape) =
    inherit CommonColumn<MtxReader>(
        (fun benchmarkCase -> benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader),
        name,
        getShape)

type Matrix2Column(name, getShape) =
    inherit CommonColumn<MtxReader*MtxReader>(
        (fun benchmarkCase -> benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader * MtxReader),
        name,
        getShape)
