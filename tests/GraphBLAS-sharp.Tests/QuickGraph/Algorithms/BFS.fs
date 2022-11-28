namespace GraphBLAS.FSharp.Tests.QuickGraph.Algorithms

open System.Collections.Generic
open QuickGraph
open QuickGraph.Algorithms.Search

module BFS =
    let runUndirected (graph: IUndirectedGraph<int, Edge<int>>) source =
        let parents = Dictionary<int, int>()
        let distances = Dictionary<int, int>()
        let mutable currentDistance = 1
        let mutable currentVertex = 0

        let bfs =
            UndirectedBreadthFirstSearchAlgorithm(graph)

        bfs.add_DiscoverVertex
            (fun x ->
                if x = source then
                    currentVertex <- source)

        bfs.add_ExamineVertex
            (fun x ->
                currentVertex <- x

                if distances.[x] = currentDistance + 1 then
                    currentDistance <- currentDistance + 1)

        bfs.add_TreeEdge
            (fun sender x ->
                let mutable source = x.Source
                let mutable target = x.Target

                if bfs.VertexColors.[target] = GraphColor.Gray then
                    let temp = source
                    source <- target
                    target <- temp

                parents.[x.Target] <- x.Source
                distances.[x.Target] <- distances.[x.Source] + 1)

        distances.[source] <- currentDistance

        bfs.Compute(source)

        parents, distances

    let runDirected (graph: AdjacencyGraph<int, Edge<int>>) source =
        let parents = Dictionary<int, int>()
        let distances = Dictionary<int, int>()
        let mutable currentDistance = 1
        let mutable currentVertex = 0
        let bfs = BreadthFirstSearchAlgorithm(graph)

        bfs.add_DiscoverVertex
            (fun x ->
                if x = source then
                    currentVertex <- source)

        bfs.add_ExamineVertex
            (fun x ->
                currentVertex <- x

                if distances.[x] = currentDistance + 1 then
                    currentDistance <- currentDistance + 1)

        bfs.add_TreeEdge
            (fun x ->
                parents.[x.Target] <- x.Source
                distances.[x.Target] <- distances.[x.Source] + 1)

        distances.[source] <- currentDistance

        bfs.Compute(source)

        parents, distances
