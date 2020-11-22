namespace GraphBLAS.FSharp

open GraphBLAS.FSharp.Predefined

module Algorithms =
    let SSSP (matrix: Matrix<float>) (source: int) : Vector<float> =
        let vertexCount = matrix.RowCount
        let distance = SparseVector(vertexCount, [source, 0.])
        for i in 0 .. vertexCount - 1 do
            distance.[Mask1D.none] <- (distance +.* matrix) Mask1D.none FloatSemiring.minPlus

        upcast distance
