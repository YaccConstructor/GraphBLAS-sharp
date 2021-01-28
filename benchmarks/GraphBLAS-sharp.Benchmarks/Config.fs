namespace GraphBLAS.FSharp.Benchmarks

open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Running
open BenchmarkDotNet.Filters
open GraphBLAS.FSharp

type InputMatrixFormat = {
    MatrixName: string
    MatrixStructure: COOFormat<float32>
}
with
    override this.ToString() =
        sprintf "%s" this.MatrixName

type MatrixShapeColumn(columnName: string, getShape: InputMatrixFormat -> int) =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Params
        member this.ColumnName: string = columnName
        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase): string =
            let inputMatrix = benchmarkCase.Parameters.["InputMatrix"] :?> InputMatrixFormat
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
            let inputMatrix = benchmarkCase.Parameters.["InputMatrix"] :?> InputMatrixFormat
            let (nrows, ncols, nnz) =
                inputMatrix.MatrixStructure.RowCount,
                inputMatrix.MatrixStructure.ColumnCount,
                inputMatrix.MatrixStructure.Values.Length
            let (vertices, edges) = if nrows = ncols then (nrows, nnz) else (ncols, nrows)
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

type Config() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", fun matrix -> matrix.MatrixStructure.RowCount) :> IColumn,
            MatrixShapeColumn("ColumnCount", fun matrix -> matrix.MatrixStructure.ColumnCount) :> IColumn,
            MatrixShapeColumn("NNZ", fun matrix -> matrix.MatrixStructure.Values.Length) :> IColumn,
            TEPSColumn() :> IColumn
        )
        |> ignore

        base.AddFilter(
            DisjunctionFilter(
                // NameFilter(fun name -> name.Contains "MathNet") :> IFilter,
                NameFilter(fun name -> name.Contains "COO") :> IFilter
            )
        )
        |> ignore
