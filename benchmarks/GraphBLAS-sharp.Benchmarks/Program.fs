open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks = BenchmarkSwitcher [|
        typeof<EWiseAddBenchmarks4Float32>
        typeof<EWiseAddBenchmarks4Bool>
    |]

    benchmarks.Run argv |> ignore
    0
