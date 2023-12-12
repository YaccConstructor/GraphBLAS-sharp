open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<Matrix.Kronecker.WithoutTransfer.Float32> |]

    benchmarks.Run argv |> ignore
    0
