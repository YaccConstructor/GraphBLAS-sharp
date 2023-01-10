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
        workGroupSize
        =

        let spMVTo =
            SpMV.runTo clContext add mul workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let ofList = Vector.ofList clContext Dense

        let maskComplementedTo =
            DenseVector.DenseVector.elementWiseTo clContext StandardOperations.complementedMaskOp workGroupSize

        let fillSubVectorTo =
            DenseVector.DenseVector.standardFillSubVectorTo<int, int> clContext workGroupSize

        let containsNonZero =
            DenseVector.DenseVector.containsNonZero clContext workGroupSize

        fun (queue: MailboxProcessor<Msg>) (matrix: CSRMatrix<'a>) (source: int) ->
            let vertexCount = matrix.RowCount

            let levels = zeroCreate queue vertexCount

            let frontier = ofList vertexCount [ source, 1 ]

            match frontier with
            | ClVectorDense front ->

                let mutable level = 0
                let mutable stop = false

                while not stop do
                    level <- level + 1

                    //Assigning new level values
                    fillSubVectorTo queue levels front (clContext.CreateClCell level) levels

                    //Getting new frontier
                    spMVTo queue matrix front front

                    maskComplementedTo queue front levels front

                    //Checking if front is empty
                    let frontNotEmpty = Array.zeroCreate 1
                    let sum = containsNonZero queue front

                    queue.PostAndReply(fun ch -> Msg.CreateToHostMsg(sum, frontNotEmpty, ch))
                    |> ignore

                    stop <- not frontNotEmpty.[0]

                front.Dispose queue

                levels
            | _ -> failwith "Not implemented"
