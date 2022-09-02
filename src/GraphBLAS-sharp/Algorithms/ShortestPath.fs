namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp

module ShortestPath =
    // TODO Unsupported call: min
    let singleSource (matrix: Mat<float>) (source: int) = graphblas {
        let vertexCount = Matrix.rowCount matrix
        let! distance = Vector.ofList vertexCount [source, 0.]

        let! transposed = Matrix.transpose matrix // A'

        // TODO terminate earlier if we reach a fixed point
        for _ = 1 to vertexCount - 1 do
            do! Matrix.mxv MinAdd.float transposed distance
            >>= Vector.assignVector distance

        return distance
    }
