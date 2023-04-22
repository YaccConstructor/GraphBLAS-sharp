module GraphBLAS.FSharp.Benchmarks.Configs

open BenchmarkDotNet.Columns
open BenchmarkDotNet.Toolchains.InProcess.Emit
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Jobs
open GraphBLAS.FSharp.Benchmarks.Columns

type Matrix2() =
    inherit ManualConfig()

    do
        base.AddColumn(
            Matrix2Column("RowCount", (fun (matrix,_) -> matrix.ReadMatrixShape().RowCount)) :> IColumn,
            Matrix2Column("ColumnCount", (fun (matrix,_) -> matrix.ReadMatrixShape().ColumnCount)) :> IColumn,
            Matrix2Column(
                "NNZ",
                fun (matrix,_) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            Matrix2Column(
                "SqrNNZ",
                fun (_,matrix) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

type Matrix() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixColumn("RowCount", (fun matrix -> matrix.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixColumn("ColumnCount", (fun matrix -> matrix.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixColumn(
                "NNZ",
                fun matrix ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

        base.AddJob(
            Job
                .Dry
                .WithToolchain(InProcessEmitToolchain.Instance)
                .WithWarmupCount(3)
                .WithIterationCount(10)
                .WithInvocationCount(3)
        )
        |> ignore

type MinMaxMean() =
    inherit ManualConfig()

    do
        base.AddColumn(
            StatisticColumn.Min,
            StatisticColumn.Max,
            StatisticColumn.Mean
        )
        |> ignore
