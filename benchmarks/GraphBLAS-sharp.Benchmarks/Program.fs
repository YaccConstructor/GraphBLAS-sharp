open BenchmarkDotNet.Running
open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Order

[<EntryPoint>]
let main argv =
    let benchmarks = BenchmarkSwitcher [|
        typeof<EWiseAddBenchmarks4Float32>
        typeof<EWiseAddBenchmarks4Bool>
        typeof<ToHostBenchmarks4Float32>
    |]

    benchmarks.Run argv |> ignore
    0
