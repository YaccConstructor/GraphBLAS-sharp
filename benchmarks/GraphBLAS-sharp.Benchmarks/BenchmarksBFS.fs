namespace GraphBLAS.FSharp.Benchmarks

open GraphBLAS.FSharp
open GraphBLAS.FSharp.Algorithms
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open System.IO
open System
open MatrixBackend

[<SimpleJob(targetCount=10)>]
type BFSBenchmark4CSRMatrix() =
    let random = Random()

    let mutable matrix = Unchecked.defaultof<Matrix<bool>>
    let mutable source = 0

    [<ParamsSource("GraphPaths")>]
    member val PathToGraph = "" with get, set

    [<GlobalSetup>]
    member this.BuildMatrix() =
        matrix <- Matrix.Build<bool>(this.PathToGraph, CSR)
        source <- random.Next matrix.RowCount

    [<Benchmark>]
    member this.LevelBFS() =
        levelBFS matrix source

    /// Sequence of paths to files where data for benchmarking will be taken from
    static member GraphPaths = seq {
        // Gets all mtx files from following directory
        yield! Directory.EnumerateFiles(Path.Join [|"Datasets"; "1"|], "*.mtx")
    }
