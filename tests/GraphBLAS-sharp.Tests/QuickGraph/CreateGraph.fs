namespace GraphBLAS.FSharp.Tests.QuickGraph

open QuikGraph

module CreateGraph =
    let directedFromArray2D (matrix: 'a [,]) zero =
        let graph = AdjacencyGraph<int, Edge<int>>()

        for outVertex in 0 .. Array2D.length1 matrix - 1 do
            for inVertex in 0 .. Array2D.length2 matrix - 1 do
                if matrix.[outVertex, inVertex] <> zero then
                    graph.AddVerticesAndEdge(Edge<int>(outVertex, inVertex))
                    |> ignore

        graph

    let undirectedFromArray2D (matrix: 'a [,]) zero =
        let edgeMatrix =
            Array2D.mapi
                (fun r c v ->
                    if v <> zero then
                        Some(Edge<int>(r, c))
                    else
                        None)
                matrix

        let edgeList =
            edgeMatrix
            |> Seq.cast<Edge<'a> option>
            |> Seq.choose id

        edgeList.ToUndirectedGraph()
