open GraphBLAS.FSharp.Benchmarks
open GraphBLAS.FSharp.Benchmarks.SpGEMM
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<SpGEMMBenchmarks4Float32CSRWithoutDataTransfer>
                             typeof<SpGEMMBenchmarks4Float32CSRWithDataTransfer>
                            //  typeof<SpGEMMBenchmarks4BoolCSRWithoutDataTransfer>
                            //  typeof<SpGEMMBenchmarks4BoolCSRWithDataTransfer>
                            //  typeof<EWiseAddBenchmarks4Float32COOWithoutDataTransfer>
                            //  typeof<EWiseAddBenchmarks4Float32COOWithDataTransfer>
                            //  typeof<EWiseAddBenchmarks4Float32CSRWithoutDataTransfer>
                            //  typeof<EWiseAddBenchmarks4BoolCOOWithoutDataTransfer>
                            //  typeof<EWiseAddBenchmarks4BoolCSRWithoutDataTransfer>
                             //typeof<BFSBenchmarks>
                             //typeof<MxvBenchmarks>
                             //typeof<TransposeBenchmarks>
                              |]

    benchmarks.Run argv |> ignore
    0
