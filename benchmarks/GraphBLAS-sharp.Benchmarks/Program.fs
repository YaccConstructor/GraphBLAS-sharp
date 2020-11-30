open BenchmarkDotNet.Running
open GraphBLAS.FSharp.Benchmarks

[<EntryPoint>]
let main argv =
    let benchmarks = BenchmarkSwitcher [|
        typeof<BfsBenchmark>
    |]

    benchmarks.Run argv |> ignore
    0
