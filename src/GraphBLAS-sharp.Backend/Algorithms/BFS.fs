namespace GraphBLAS.FSharp.Backend.Algorithms

open GraphBLAS.FSharp.Backend
open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Backend.Vector.Dense
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

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
            DenseVector.elementWiseTo clContext Mask.complementedMaskOp workGroupSize

        let fillSubVectorTo =
            DenseVector.standardFillSubVectorTo<int, int> clContext workGroupSize

        let containsNonZero =
            ClArray.exists clContext workGroupSize Predicates.containsNonZero

        fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix.CSR<'a>) (source: int) ->
            let vertexCount = matrix.RowCount

            let levels = zeroCreate queue vertexCount

            let frontier = ofList vertexCount [ source, 1 ]

            match frontier with
            | ClVector.Dense front ->

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
