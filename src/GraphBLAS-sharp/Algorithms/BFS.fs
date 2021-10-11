namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp

module BFS =
    let levelSingleSource (matrix: Matrix<int>) (source: int) = graphblas {
        let vertexCount = Matrix.rowCount matrix
        let! levels = Vector.zeroCreate vertexCount // v
        let! frontier = Vector.ofList vertexCount [source, 1] // q[s] = true
        let! transposed = Matrix.transpose matrix // A'

        let mutable currentLevel = 0
        let mutable break' = false
        while not break' do
            currentLevel <- currentLevel + 1

            let! currentLevelScalar = Scalar.create currentLevel

            let! frontierMask = Vector.mask frontier
            do! Vector.fillSubVector levels frontierMask currentLevelScalar // v[q] = d

            let! levelsComplemented = Vector.complemented levels
            do! Matrix.mxvWithMask AddMult.int levelsComplemented transposed frontier // q[!v] = (A' ||.&& q)' = q' ||.&& A -- replace + comp
            >>= Vector.assignVector frontier

            let! succ =
                Vector.reduce AddMult.int frontier
                >>= Scalar.exportValue

            break' <- succ = 0

        return levels
    }
