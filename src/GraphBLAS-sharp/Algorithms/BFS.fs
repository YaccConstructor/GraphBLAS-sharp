namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp.Helpers
open GraphBLAS.FSharp

[<AutoOpen>]
module BFS =
    let levelBFS (matrix: Matrix<bool>) (source: int) : Vector<int> =
        let vertexCount = matrix.RowCount
        let levels = DenseVector(Array.zeroCreate vertexCount, IntegerMonoid.add)

        let frontier = SparseVector(vertexCount, [source, true])

        let mutable currentLevel = 1
        while !> (frontier.Reduce BooleanMonoid.any) && currentLevel < vertexCount do
            levels.Fill(frontier.Mask) <- Scalar currentLevel
            frontier.Clear()
            frontier.[levels.Complemented] <- (frontier @. matrix) levels.Complemented BooleanSemiring.anyAll
            currentLevel <- currentLevel + 1

        upcast levels

    // let parentBFS (matrix: Matrix<bool>) (source: int) : Vector<int> =
    //     let vertexCount = matrix.RowCount
    //     let parents = SparseVector(vertexCount, [source, -1])

    //     let id = DenseVector(Array.init vertexCount id, IntegerMonoid.add)
    //     let frontier = SparseVector(vertexCount, [source, source])

    //     for _ in 1 .. vertexCount - 1 do
    //         frontier.[parents.Complemented] <- (frontier @. matrix) parents.Complemented IntegerSemiring.minFirst
    //         parents.[frontier.Mask] <- frontier
    //         frontier.[frontier.Mask] <- id

    //     upcast parents
