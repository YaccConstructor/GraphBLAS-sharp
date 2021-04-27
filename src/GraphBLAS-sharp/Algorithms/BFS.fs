namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp

module BFS =
    let levelSingleSource (matrix: Matrix<bool>) (source: int) = graphblas {
        let vertexCount = Matrix.rowCount matrix
        let! levels = Vector.zeroCreate vertexCount // v
        let! frontier = Vector.ofList vertexCount [source, true] // q[s] = true

        let mutable currentLevel = 0
        let mutable break' = false
        while not break' do
            currentLevel <- currentLevel + 1

            // NOTE mask application is ugly
            let! frontierMask = Vector.mask frontier
            do! Scalar currentLevel |> Vector.fillSubVector levels frontierMask // v[q] = d

            let! levelsComplemented = Vector.complemented levels
            do! (frontier, matrix) ||> Vector.vxmWithMask AnyAll.bool levelsComplemented // q[!v] = q ||.&& A -- replace + comp
            >>= Vector.assignVector frontier

            // TODO need export or sync for scalar
            let! (Scalar succ) = frontier |> Vector.reduce AnyAll.bool
            break' <- not succ

        return levels
    }
