namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

module TriangleCounting =
    let sandia (lowerTriangular: Matrix<bool>) = graphblas {
        let bool2int = function
            | true -> 1
            | false -> 0

        let! convertedMatrix = lowerTriangular |> Matrix.apply (UnaryOp <@ bool2int @>)
        let! convertedTransposed = convertedMatrix |> Matrix.transpose
        let! lowerTriangularMask = lowerTriangular |> Matrix.mask
        let! result = Matrix.mxmWithMask AddMult.int lowerTriangularMask convertedMatrix convertedTransposed
        let! (Scalar count) = result |> Matrix.reduce Add.int
        return count
    }
