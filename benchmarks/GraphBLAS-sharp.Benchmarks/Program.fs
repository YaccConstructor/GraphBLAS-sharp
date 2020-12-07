open BenchmarkDotNet.Running
open GraphBLAS.FSharp.Benchmarks

[<EntryPoint>]
let main argv =
    let benchmarks = BenchmarkSwitcher [|
        typeof<BFSBenchmark4CSRMatrix>
    |]

    benchmarks.Run argv |> ignore
    0
