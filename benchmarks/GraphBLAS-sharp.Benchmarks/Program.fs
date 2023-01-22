open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<BFSBenchmarksWithoutDataTransfer>
                             typeof<MxmBenchmarks4Float32MultiplicationOnly>
                             //typeof<MxvBenchmarks>
                             //typeof<TransposeBenchmarks>
                              |]

    benchmarks.Run argv |> ignore
    0
