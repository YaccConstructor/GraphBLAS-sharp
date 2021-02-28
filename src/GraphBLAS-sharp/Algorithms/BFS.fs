namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp.Helpers
open GraphBLAS.FSharp
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<AutoOpen>]
module BFS =
    let levelBFS (matrix: Matrix<bool>) (source: int) : OpenCLEvaluation<Vector<int>> =
        let vertexCount = matrix.RowCount
        let levels = Vector.Dense(Array.zeroCreate vertexCount, IntegerMonoid.add)
        let frontier = Vector.Sparse(vertexCount, [source, true])

        opencl {
            let mutable currentLevel = 1
            while currentLevel < vertexCount do
                let! frontierMask = frontier.GetMask()
                do! levels.Assign(frontierMask, Scalar currentLevel)
                let! levelsComplemented = levels.GetMask(isComplemented = true)
                let! frontier = frontier.Vxm matrix levelsComplemented BooleanSemiring.anyAll
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
