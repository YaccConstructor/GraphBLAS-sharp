open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<EWiseAddBenchmarks4Float32COO>
                             typeof<EWiseAddBenchmarks4Float32COOWithDataTransfer>
                             //typeof<EWiseAddBenchmarks4Float32CSR>
                             //typeof<EWiseAddBenchmarks4BoolCOO>
                             //typeof<EWiseAddBenchmarks4BoolCSR>
                             //typeof<BFSBenchmarks>
                             //typeof<MxvBenchmarks>
                             //typeof<TransposeBenchmarks>
                              |]

    benchmarks.Run argv |> ignore
    0
