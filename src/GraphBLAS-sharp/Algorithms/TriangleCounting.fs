namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<AutoOpen>]
module TriangleCounting =
    let sandiaTriangleCount (lowerTriangular: Matrix<bool>) : GraphblasEvaluation<int> =
        let bool2int = function
            | true -> 1
            | false -> 0

        graphblas {
            let! convertedMatrix = lowerTriangular |> Matrix.apply (UnaryOp <@ bool2int @>) None
            let! convertedTransposed = convertedMatrix |> Matrix.transpose
            let! lowerTriangularMask = lowerTriangular |> Matrix.mask
            let! result = Matrix.mxm AddMult.int lowerTriangularMask convertedMatrix convertedTransposed
            let! (Scalar count) = result |> Matrix.reduce Add.int
            return count
        }
