namespace GraphBLAS.FSharp.Benchmarks

open GraphBLAS.FSharp
open GraphBLAS.FSharp.Algorithms
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open System.IO
open System
open System.IO
open System.Text.RegularExpressions
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Predefined
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open BenchmarkDotNet.Filters
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open OpenCL.Net
open GraphBLAS.FSharp.IO
open QuickGraph

type Config2() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", fun mtxReader -> mtxReader.ReadMatrixShape().RowCount) :> IColumn,
            MatrixShapeColumn("ColumnCount", fun mtxReader -> mtxReader.ReadMatrixShape().ColumnCount) :> IColumn,
            MatrixShapeColumn("NNZ", fun mtxReader -> mtxReader.ReadMatrixShape().Nnz) :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        ) |> ignore

[<IterationCount(5)>]
[<WarmupCount(3)>]
[<Config(typeof<Config2>)>]
type QGBenchmarks() =
    let random = Random()

    let graph = AdjacencyGraph<int, Edge<int>>(false)
    let mutable source = 0
    let mutable bfs = Unchecked.defaultof<Algorithms.Search.BreadthFirstSearchAlgorithm<int, Edge<int>>>

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader> with get, set

    [<GlobalSetup>]
    member this.BuildMatrixAndSetSource() =
        let matrix = this.InputMatrixReader.ReadMatrix(fun _ -> true)

        match matrix with
        | MatrixCSR csr -> failwith "Not implemented"
        | MatrixCOO coo ->
            for i = 0 to coo.Values.Length - 1 do
                graph.AddVerticesAndEdge(Edge(coo.Rows.[i], coo.Columns.[i])) |> ignore

        source <- random.Next <| Matrix.rowCount matrix
        bfs <- Algorithms.Search.BreadthFirstSearchAlgorithm(graph)

    [<Benchmark>]
    member this.LevelBFS() =
        bfs.Compute(source)

    static member InputMatricesProvider =
        "BFSBenchmarks.txt"
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix "BFSDatasets" matrixFilename)
                | _ -> failwith "Unsupported matrix format"
            )
