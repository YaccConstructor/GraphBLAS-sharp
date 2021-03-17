namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<AutoOpen>]
module SSSP =
    let SSSP (matrix: Matrix<float>) (source: int) : GraphblasEvaluation<Vector<float>> =
        let vertexCount = Matrix.rowCount matrix
        let distance = Vector.ofTuples vertexCount [source, 0.]

        graphblas {
            for _ in 1 .. vertexCount - 1 do
                let! step = Vector.vxm MinAdd.float None distance matrix
                do! distance |> Vector.assignSubVector None step

            return distance
        }
