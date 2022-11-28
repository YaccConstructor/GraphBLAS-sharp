namespace GraphBLAS.FSharp.Tests.QuickGraph.Algorithms

open System.Collections.Generic
open QuickGraph
open QuickGraph.Algorithms.ConnectedComponents
open QuickGraph.Algorithms.Search

module ConnectedComponents =
    let runUndirected (graph: IUndirectedGraph<int, Edge<int>>) =
        let cc = ConnectedComponentsAlgorithm(graph)

        cc.Compute()

        cc.Components
        |> Seq.groupBy (fun kv -> kv.Value)
        |> Seq.map
            (fun (comp, vertices) ->
                comp,
                vertices
                |> Seq.map (fun kv -> kv.Key)
                |> Array.ofSeq)

    let largestComponent graph =
        let components = runUndirected graph

        if Seq.isEmpty components then
            [||]
        else
            components
            |> Seq.maxBy (fun (comp, vertices) -> vertices.Length)
            |> snd
