namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp

module TriangleCounting =
    let sandia (matrix: Matrix<bool>) = graphblas {
        let! lowerTriangular = matrix |> Matrix.select (UnaryOp <@ fun (i, j, _) -> i <= j @>)
        let! matrix' = lowerTriangular |> Matrix.apply (UnaryOp <@ function | true -> 1 | false -> 0 @>)
        let! transposed = matrix' |> Matrix.transpose

        let! lowerTriangularMask = lowerTriangular |> Matrix.mask
        let! (Scalar count) =
            (matrix', transposed)
            ||> Matrix.mxmWithMask AddMult.int lowerTriangularMask
            >>= Matrix.reduce Add.int

        return count
    }
