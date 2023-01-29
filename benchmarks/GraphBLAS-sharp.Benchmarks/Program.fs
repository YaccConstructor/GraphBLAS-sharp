open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<BFSBenchmarksWithoutDataTransfer>
                             typeof<EWiseAddAtLeastOneBenchmarks4Float32CSRWithoutDataTransfer>
                              |]

    benchmarks.Run argv |> ignore
    0
