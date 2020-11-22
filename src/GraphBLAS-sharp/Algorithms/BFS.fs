namespace GraphBLAS.FSharp

open GraphBLAS.FSharp.Predefined
open Helpers

module Algorithms =
    let levelBFS (matrix: Matrix<bool>) (source: int) : Vector<int> =
        let vertexCount = matrix.RowCount
        let levels = DenseVector(Array.zeroCreate vertexCount, IntegerMonoid.plus)
        let frontier = SparseVector(vertexCount, [source, true])

        let mutable currentLevel = 1
        while !> (frontier.Reduce BooleanMonoid.any) && currentLevel <= vertexCount do
            levels.Fill(Mask1D.regular frontier) <- Scalar currentLevel
            frontier.Clear()
            frontier.[Mask1D.complemented levels] <- (frontier +.* matrix) (Mask1D.complemented levels) BooleanSemiring.anyAll
            currentLevel <- currentLevel + 1

        upcast levels

    let parentBFS (matrix: Matrix<bool>) (source: int) : Vector<int> =
        let vertexCount = matrix.RowCount
        let id = DenseVector(Array.init vertexCount id, IntegerMonoid.plus)
        let frontier = SparseVector(vertexCount, [source, source])
        let parents = SparseVector(vertexCount, [source, -1])

        for i in 0 .. vertexCount - 1 do
            frontier.[Mask1D.complemented parents] <- (frontier +.* matrix) (Mask1D.complemented parents) IntegerSemiring.minFirst
            parents.[Mask1D.regular frontier] <- frontier
            frontier.[Mask1D.regular frontier] <- id

        upcast parents
