namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp.Helpers
open GraphBLAS.FSharp
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<AutoOpen>]
module Ext =
    type OpenCLEvaluationBuilder with
        member this.While(guard, body) =
            if not <| guard ()
            then this.Zero()
            else this.Bind(body, fun () -> this.While(guard, body))

        member this.Delay(f) = f ()

        member this.Zero() = this.Return()

        member this.Combine(a, b) = this.Bind(a, fun () -> b)

[<AutoOpen>]
module BFS =
    let levelBFS (matrix: Matrix<bool>) (source: int) : OpenCLEvaluation<Vector<int>> =
        let vertexCount = matrix.RowCount
        let levels = Vector.Dense(Array.zeroCreate vertexCount, IntegerMonoid.add)

        let frontier = Vector.Sparse(vertexCount, [source, true])
        let mutable currentLevel = 1

        // let inline ($) (a: Mask1D option -> 'a) (b: Mask1D option) = a <| b
        let inline (?<-) (a: Vector<'a>) (b: Mask1D option) (c: Scalar<'a>) = a.Assign(b, c)
        let inline (<@@>) (a: Vector<'a>) (b: Mask1D option) (c: Scalar<'a>) = ()



        // let frontier = (@.) frontier

        // let a = levels.AssignE <| frontier.Mask
        // let a = levels $ frontier.Mask <| Scalar currentLevel
        let a = levels ? frontier.Mask <- Scalar currentLevel




        // opencl {
        //     while currentLevel < vertexCount do
        //         do! levels.Assign(frontier.Mask, Scalar currentLevel)
        //         // let! frontier = frontier @. matrix <|| (levels.Complemented, BooleanSemiring.anyAll)
        //         let! frontier = frontier @. matrix <| {| Mask = levels.Complemented; SR = BooleanSemiring.anyAll |}
        //         currentLevel <- currentLevel + 1
        //         // let! a = frontier.Reduce BooleanMonoid.any
        //         // let s = !> a
        //         let a = levels.Assign $ frontier.Mask

        //     return levels
        // }

        // while !> (frontier.Reduce BooleanMonoid.any) && currentLevel < vertexCount do
        //     levels.Assign(frontier.Mask, Scalar currentLevel)
        //     frontier.Clear()
        //     frontier.Assign(levels.Complemented, (frontier @. matrix) levels.Complemented BooleanSemiring.anyAll)
        //     currentLevel <- currentLevel + 1

        // upcast levels



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
