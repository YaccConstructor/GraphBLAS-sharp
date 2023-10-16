namespace GraphBLAS.FSharp.Tests.Backend.QuickGraph.Algorithms

open QuikGraph
open QuikGraph.Algorithms.ShortestPath
open QuikGraph.Algorithms.Observers

module SSSP =
    let runUndirected (matrix: int [,]) (graph: AdjacencyGraph<int, Edge<int>>) source =
        let weight =
            fun (e: Edge<int>) -> float matrix.[e.Source, e.Target]

        let dijkstra =
            DijkstraShortestPathAlgorithm<int, Edge<int>>(graph, weight)

        // Attach a distance observer to give us the shortest path distances
        let distObserver =
            VertexDistanceRecorderObserver<int, Edge<int>>(weight)

        distObserver.Attach(dijkstra) |> ignore

        // Attach a Vertex Predecessor Recorder Observer to give us the paths
        let predecessorObserver =
            VertexPredecessorRecorderObserver<int, Edge<int>>()

        predecessorObserver.Attach(dijkstra) |> ignore

        // Run the algorithm with A set to be the source
        dijkstra.Compute(source)

        let res: array<float option> =
            Array.zeroCreate (Array2D.length1 matrix)

        for kvp in distObserver.Distances do
            res.[kvp.Key] <- Some kvp.Value

        res.[source] <- Some 0.0
        res
