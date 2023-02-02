open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<EWiseAddBenchmarks4Float32COOWithoutDataTransfer>
                             typeof<EWiseAddBenchmarks4Float32COOWithDataTransfer>
                             typeof<EWiseAddBenchmarks4Float32CSRWithoutDataTransfer>
                             typeof<EWiseAddBenchmarks4BoolCOOWithoutDataTransfer>
                             typeof<EWiseAddBenchmarks4BoolCSRWithoutDataTransfer>
                             typeof<MxmBenchmarks4Float32MultiplicationOnly>
                             typeof<MxmBenchmarks4Float32WithTransposing>
                             typeof<MxmBenchmarks4BoolMultiplicationOnly>
                             typeof<MxmBenchmarks4BoolWithTransposing>
                             typeof<VectorEWiseBenchmarks4Int32SparseWithoutDataTransfer>
                             typeof<VectorEWiseGeneralBenchmarks4Int32SparseWithoutDataTransfer>
                             typeof<VectorEWiseBenchmarks4FloatSparseWithoutDataTransfer>
                             typeof<VectorEWiseGeneralBenchmarks4FloatSparseWithoutDataTransfer>
                             //typeof<BFSBenchmarks>
                             //typeof<MxvBenchmarks>
                             //typeof<TransposeBenchmarks>
                              |]

    benchmarks.Run argv |> ignore
    0
