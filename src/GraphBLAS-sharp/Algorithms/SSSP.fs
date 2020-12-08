namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp

[<AutoOpen>]
module SSSP =
    let SSSP (matrix: Matrix<float>) (source: int) : Vector<float> =
        let vertexCount = matrix.RowCount
        let distance = SparseVector(vertexCount, [source, 0.])

        for _ in 1 .. vertexCount - 1 do
            distance.[None] <- (distance @. matrix) None FloatSemiring.addMult

        upcast distance
