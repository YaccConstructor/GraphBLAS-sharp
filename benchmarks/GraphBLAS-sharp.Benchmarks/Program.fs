open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<Algorithms.BFS.BFSWithoutTransferBenchmarkBool>
                             typeof<Algorithms.BFS.BFSPushPullWithoutTransferBenchmarkBool>
                             typeof<Algorithms.PageRank.PageRankWithoutTransferBenchmarkFloat32> |]

    benchmarks.Run argv |> ignore
    0
