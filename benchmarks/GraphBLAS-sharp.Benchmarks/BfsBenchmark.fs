namespace GraphBLAS.FSharp.Benchmarks

open GraphBLAS.FSharp
open GraphBLAS.FSharp.Algorithms
open BenchmarkDotNet.Attributes
open System.IO

type BfsBenchmark() =
    let mutable matrix = Unchecked.defaultof<Matrix<bool>>

    [<ParamsSource("GraphPaths")>]
    member val PathToGraph = "" with get, set

    [<GlobalSetup>]
    member this.BuildMatrix () =
        matrix <- Matrix.Build<bool> this.PathToGraph

    [<Benchmark>]
    member this.Bfs () =
        levelBFS matrix 0

    static member GraphPaths = seq {
        yield! Directory.EnumerateFiles(Path.Join [|"Datasets"; "1"|], "*.mtx")
    }
