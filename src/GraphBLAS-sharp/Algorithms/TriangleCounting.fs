namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp

module TriangleCounting =
    let sandia (matrix: Matrix<bool>) = graphblas {
        let! lowerTriangular = matrix |> Matrix.select (UnaryOp <@ fun (i, j, _) -> i <= j @>)
        let! matrix' = lowerTriangular |> Matrix.apply (UnaryOp <@ function | true -> 1 | false -> 0 @>)
        let! transposed = matrix' |> Matrix.transpose

        let! lowerTriangularMask = lowerTriangular |> Matrix.mask
        let! result = (matrix', transposed) ||> Matrix.mxmWithMask AddMult.int lowerTriangularMask
        let! count = result |> Matrix.reduce Add.int

        do! Scalar.synchronize count

        return! (Scalar.extractValue count)
    }
