namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

module ShortestPath =
    let singleSource (matrix: Matrix<float>) (source: int) = graphblas {
        let vertexCount = Matrix.rowCount matrix
        let! distance = Vector.ofList vertexCount [source, 0.]

        // TODO terminate earlier if we reach a fixed point
        for _ = 1 to vertexCount - 1 do
            do! Vector.vxm MinAdd.float distance matrix
            >>= Vector.assignVector distance

        return distance
    }
