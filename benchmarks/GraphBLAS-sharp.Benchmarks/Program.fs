open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<Algorithms.BFS.WithoutTransfer.Int> |]

    benchmarks.Run argv |> ignore
    0
