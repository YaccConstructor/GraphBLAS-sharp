open BenchmarkDotNet.Configs
open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks = BenchmarkSwitcher [|
        typeof<EWiseAddBenchmarks4Float32COOWithoutDataTransfer>
        typeof<EWiseAddBenchmarks4Float32COOWithDataTransfer>
        typeof<EWiseAddBenchmarks4Float32CSRWithoutDataTransfer>
        typeof<EWiseAddBenchmarks4BoolCOOWithoutDataTransfer>
        typeof<EWiseAddBenchmarks4BoolCSRWithoutDataTransfer>
        typeof<Concrete.EWiseAddBatchedIntBenchmarks>
        //typeof<BFSBenchmarks>
        //typeof<MxvBenchmarks>
        //typeof<TransposeBenchmarks>
    |]

    benchmarks.Run argv |> ignore
    0
