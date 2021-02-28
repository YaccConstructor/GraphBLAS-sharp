namespace GraphBLAS.FSharp.Benchmarks

open GraphBLAS.FSharp
open GraphBLAS.FSharp.Algorithms
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open System.IO
open System

[<SimpleJob(targetCount=10)>]
type BFSBenchmark4CSRMatrix() =
    let random = Random()

    let mutable matrix = Unchecked.defaultof<Matrix<bool>>
    let mutable source = 0

    [<ParamsSource("GraphPaths")>]
    member val PathToGraph = "" with get, set

    [<GlobalSetup>]
    member this.BuildMatrix() =
        matrix <- CSRMatrix(this.PathToGraph)
        source <- random.Next matrix.RowCount

    [<Benchmark>]
    member this.LevelBFS() =
        levelBFS matrix source

    /// Sequence of paths to files where data for benchmarking will be taken from
    static member GraphPaths = seq {
        // Gets all mtx files from following directory
        yield! Directory.EnumerateFiles(Path.Combine [|"Datasets"; "1"|], "*.mtx")
    }
