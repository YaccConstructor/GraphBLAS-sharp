open System
open BenchmarkDotNet.Running

type A = class end

[<EntryPoint>]
let main argv =
    let benchmarks = BenchmarkSwitcher [| typeof<A> |]
    benchmarks.Run argv |> ignore
    0
