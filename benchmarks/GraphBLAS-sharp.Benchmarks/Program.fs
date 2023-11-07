open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<Algorithms.BFS.BFSWithoutTransferBenchmarkInt32>
                             typeof<Algorithms.BFS.BFSPushPullWithoutTransferBenchmarkInt32>
                             typeof<Algorithms.PageRank.PageRankWithoutTransferBenchmarkFloat32> |]

    benchmarks.Run argv |> ignore
    0
