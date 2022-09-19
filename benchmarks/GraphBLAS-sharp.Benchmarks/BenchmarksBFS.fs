namespace GraphBLAS.FSharp.Benchmarks

open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Algorithms
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open System.IO
open System
open System.Text.RegularExpressions
open Brahma.FSharp.OpenCL
open OpenCL.Net
open GraphBLAS.FSharp.IO
open QuickGraph

[<Config(typeof<CommonConfig>)>]
type BFSBenchmarks() =
    let random = Random()

    let mutable source = 0

    // gb
    let mutable matrix = Unchecked.defaultof<Mat<int>>

    // qg
    let graph = AdjacencyGraph<int, Edge<int>>(false)
    let mutable bfs = Unchecked.defaultof<Algorithms.Search.BreadthFirstSearchAlgorithm<int, Edge<int>>>

    [<ParamsSource("AvaliableContextsProvider")>]
    member val Context = Unchecked.defaultof<RuntimeContext> with get, set
//    member this.Context = failwith "fix me"
//    //let (ClContext context) = this.OclContext
//    //context

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader> with get, set

    [<GlobalSetup>]
    member this.BuildGraph() =
        let inputMatrix = this.InputMatrixReader.ReadMatrix(fun _ -> 1)
        let deviceMatrix = inputMatrix.ToDevice(this.Context.ClContext)

        matrix <-
            graphblas {
                return! Matrix.switch Backend.MatrixFormat.CSR inputMatrix
                >>= Matrix.synchronizeAndReturn
            }
            |> EvalGB.withClContext this.Context
            |> EvalGB.runSync

        match inputMatrix with
        | Backend.MatrixCSR csr -> failwith "Not implemented"
        | Backend.MatrixCOO coo ->
            for i = 0 to coo.Values.Length - 1 do
                graph.AddVerticesAndEdge(Edge(coo.Rows.[i], coo.Columns.[i])) |> ignore

        bfs <- Algorithms.Search.BreadthFirstSearchAlgorithm(graph)

    [<IterationSetup>]
    member this.SetSource() = source <- random.Next <| Matrix.rowCount matrix

    [<Benchmark>]
    member this.GraphblasLevelBFS() =
        BFS.levelSingleSource matrix source
        |> EvalGB.withClContext this.Context
        |> EvalGB.runSync

    [<Benchmark>]
    member this.QuickGraphBFS() = bfs.Compute(source)

    static member AvaliableContextsProvider = Utils.avaliableContexts

    static member InputMatricesProvider =
        "Common.txt"
        |> Utils.getMatricesFilenames
        |> Seq.map (fun matrixFilename ->
            match Path.GetExtension matrixFilename with
            | ".mtx" -> MtxReader(Utils.getFullPathToMatrix "Common" matrixFilename)
            | _ -> failwith "Unsupported matrix format"
        )
