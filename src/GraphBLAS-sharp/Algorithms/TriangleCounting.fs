namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<AutoOpen>]
module TriangleCounting =
    // скорее всего, тут не скаляр возвращать нужно, а инт
    let sandiaTriangleCount (lowerTriangular: Matrix<bool>) : OpenCLEvaluation<Scalar<int>> =
        let bool2int = function
            | true -> 1
            | false -> 0

        opencl {
            let! convertedMatrix = lowerTriangular.Apply None (UnaryOp <@ bool2int @>)
            let! convertedTransposed = convertedMatrix.Transpose()
            let! lowerTriangularMask = lowerTriangular.GetMask()
            let! result = convertedMatrix.Mxm convertedTransposed lowerTriangularMask AddMult.int
            return! result.Reduce Add.int
        }
