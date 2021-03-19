namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp.Helpers
open GraphBLAS.FSharp
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<AutoOpen>]
module BFS =
    let levelBFS (matrix: Matrix<bool>) (source: int) : GraphblasEvaluation<Vector<int>> =
        let vertexCount = Matrix.rowCount matrix
        let levels = Vector.ofArray <| (=) 0 <| Array.zeroCreate vertexCount
        let frontier = Vector.ofList vertexCount [source, true]

        graphblas {
            let mutable currentLevel = 1
            while currentLevel < vertexCount do
                let! frontierMask = Vector.mask frontier
                do! Vector.fillSubVector frontierMask (Scalar currentLevel) levels
                let! levelsComplemented = Vector.complemented levels
                let! frontier = Vector.vxm AnyAll.bool levelsComplemented frontier matrix
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
