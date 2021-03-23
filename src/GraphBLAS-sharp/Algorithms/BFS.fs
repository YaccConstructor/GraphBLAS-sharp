namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp.Helpers
open GraphBLAS.FSharp
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

module BFS =
    let level (matrix: Matrix<bool>) (source: int) = graphblas {
        let vertexCount = Matrix.rowCount matrix
        let levels = Vector.zeroCreate vertexCount 0
        let frontier = Vector.ofList vertexCount [source, true]

        let mutable currentLevel = 1
        while currentLevel < vertexCount do
            let! frontierMask = Vector.mask frontier
            do! levels |> Vector.fillSubVector frontierMask (Scalar currentLevel)

            let! levelsComplemented = Vector.complemented levels
            let! frontier = (frontier, matrix) ||> Vector.vxmWithMask AnyAll.bool levelsComplemented

            currentLevel <- currentLevel + 1

        return levels
    }

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
