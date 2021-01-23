open BenchmarkDotNet.Running
open GraphBLAS.FSharp.Benchmarks
open System.IO
open MathNet.Numerics

[<EntryPoint>]
let main argv =
    let benchmarks = BenchmarkSwitcher [|
        typeof<EWiseAddBenchmarks>
    |]

    benchmarks.Run argv |> ignore
    0
