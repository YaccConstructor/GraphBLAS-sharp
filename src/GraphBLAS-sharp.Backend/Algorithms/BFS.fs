namespace GraphBLAS.FSharp.Backend.Algorithms

open GraphBLAS.FSharp.Backend
open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp.Backend.ArraysExtensions
open GraphBLAS.FSharp.Backend.Common

module BFS =
    let singleSource
        (clContext: ClContext)
        (add: Expr<int option -> int option -> int option>)
        (mul: Expr<'a option -> 'b option -> int option>)
        (addNumeric: Expr<int -> int -> int>)
        workGroupSize
        =

        let transpose =
            CSRMatrix.transpose clContext workGroupSize

        let spMV = SpMV.run clContext add mul workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let ofList = Vector.ofList clContext Dense

        let maskComplemented =
            DenseVector.DenseVector.elementWise clContext StandardOperations.complementedMaskOp workGroupSize

        let fillSubVector =
            Vector.standardFillSubVector<int, int> clContext workGroupSize

        let reduce =
            Vector.reduce clContext workGroupSize addNumeric

        fun (queue: MailboxProcessor<Msg>) (matrix: CSRMatrix<'a>) (source: int) ->
            let vertexCount = matrix.RowCount

            let mutable levels: ClVector<int> =
                zeroCreate queue vertexCount |> ClVectorDense

            let mutable frontier = ofList vertexCount [ source, 1 ]
            let transposedMatrix = transpose queue matrix

            let mutable level = 0
            let mutable stop = false

            while not stop do
                level <- level + 1

                let newLevels =
                    fillSubVector queue levels frontier (clContext.CreateClCell level)

                levels.Dispose queue

                match frontier, newLevels with
                | ClVectorDense f, ClVectorDense nl ->
                    let newFrontierUnmasked = spMV queue transposedMatrix f

                    let newFrontier =
                        maskComplemented queue newFrontierUnmasked nl

                    newFrontierUnmasked.Dispose queue
                    frontier.Dispose queue

                    frontier <- newFrontier |> ClVectorDense
                    levels <- newLevels

                    let frontierSum = Array.zeroCreate 1
                    let sum = reduce queue frontier

                    queue.PostAndReply(fun ch -> Msg.CreateToHostMsg(sum, frontierSum, ch))
                    |> ignore

                    stop <- frontierSum.[0] = 0

            levels
