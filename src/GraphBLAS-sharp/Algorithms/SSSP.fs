namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<AutoOpen>]
module SSSP =
    let SSSP (matrix: Matrix<float>) (source: int) : OpenCLEvaluation<Vector<float>> =
        let vertexCount = matrix.RowCount
        let distance = Vector.Sparse(vertexCount, [source, 0.])

        opencl {
            for _ in 1 .. vertexCount - 1 do
                let! step = distance.Vxm matrix None FloatSemiring.minAdd
                do! distance.Assign(None, step)

            return distance
        }
