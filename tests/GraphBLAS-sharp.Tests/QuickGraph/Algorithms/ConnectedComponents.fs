namespace GraphBLAS.FSharp.Tests.Backend.QuickGraph.Algorithms

open System.Collections.Generic
open QuikGraph
open QuikGraph.Algorithms.ConnectedComponents

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
