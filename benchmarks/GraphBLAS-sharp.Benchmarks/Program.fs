open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<Algorithms.BFSIntWithoutTransferBenchmark> |]

    benchmarks.Run argv |> ignore
    0
