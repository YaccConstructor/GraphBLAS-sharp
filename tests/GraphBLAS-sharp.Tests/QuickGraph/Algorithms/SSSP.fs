namespace GraphBLAS.FSharp.Tests.Backend.QuickGraph.Algorithms

open System.Collections.Generic
open QuikGraph
open QuikGraph.Algorithms.ShortestPath
open QuikGraph.Algorithms.Observers
open QuikGraph.Algorithms

module SSSP =
    let runUndirected (matrix: int [,]) (graph: AdjacencyGraph<int, Edge<int>>) source =
        let weight =
            fun (e: Edge<int>) -> float matrix.[e.Source, e.Target]

        let dijkstra =
            DijkstraShortestPathAlgorithm<'a, Edge<'a>>(graph, weight)

        // Attach a distance observer to give us the shortest path distances
        let distObserver =
            VertexDistanceRecorderObserver<'a, Edge<'a>>(weight)

        distObserver.Attach(dijkstra)

        // Attach a Vertex Predecessor Recorder Observer to give us the paths
        let predecessorObserver =
            new VertexPredecessorRecorderObserver<'a, Edge<'a>>()

        predecessorObserver.Attach(dijkstra)

        // Run the algorithm with A set to be the source
        dijkstra.Compute(source)

        let res: array<float option> =
            Array.zeroCreate (Array2D.length1 matrix)

        for kvp in distObserver.Distances do
            res.[kvp.Key] <- Some kvp.Value

        res.[source] <- Some 0.0
        res
