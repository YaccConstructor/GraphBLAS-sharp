namespace GraphBLAS.FSharp.Backend.Algorithms

open GraphBLAS.FSharp.Backend
open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Backend.Vector.Dense
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClCell

module BFS =
    let singleSource
        (add: Expr<int option -> int option -> int option>)
        (mul: Expr<'a option -> int option -> int option>)
        (clContext: ClContext)
        workGroupSize
        =

        let spMVTo =
            SpMV.runTo add mul clContext workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let ofList = Vector.ofList clContext workGroupSize

        let maskComplementedTo =
            Vector.map2InPlace Mask.complementedOp clContext workGroupSize

        let fillSubVectorTo =
            Vector.assignByMaskInPlace (Convert.assignToOption Mask.assign) clContext workGroupSize

        let containsNonZero =
            ClArray.exists Predicates.isSome clContext workGroupSize

        fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix.CSR<'a>) (source: int) ->
            let vertexCount = matrix.RowCount

            let levels = zeroCreate queue HostInterop vertexCount

            let frontier =
                ofList queue DeviceOnly Dense vertexCount [ source, 1 ]

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
                    stop <-
                        not
                        <| (containsNonZero queue front).ToHostAndFree queue

                front.Free queue

                levels
            | _ -> failwith "Not implemented"

    let singleSourceSparse
        (add: Expr<bool option -> bool option -> bool option>)
        (mul: Expr<bool option -> bool option -> bool option>)
        (clContext: ClContext)
        workGroupSize
        =

        let spMSpV =
            SpMSpV.run add mul clContext workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let ofList = Vector.ofList clContext workGroupSize

        let maskComplemented =
            Vector.Sparse.Vector.map2SparseDense Mask.complementedOp clContext workGroupSize

        let fillSubVectorTo =
            Vector.assignBySparseMaskInPlace (Convert.assignToOption Mask.assign) clContext workGroupSize

        fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix.CSR<bool>) (source: int) ->
            let vertexCount = matrix.RowCount

            let levels = zeroCreate queue HostInterop vertexCount

            let mutable frontier =
                ofList queue DeviceOnly Sparse vertexCount [ source, true ]

            let mutable level = 0
            let mutable stop = false

            while not stop do
                match frontier with
                | ClVector.Sparse front ->
                    level <- level + 1

                    //Assigning new level values
                    fillSubVectorTo queue levels front (clContext.CreateClCell level) levels

                    //Getting new frontier
                    match spMSpV queue matrix front with
                    | None ->
                        frontier.Dispose queue
                        stop <- true
                    | Some newFrontier ->
                        frontier.Dispose queue
                        //Filtering visited vertices
                        match maskComplemented queue DeviceOnly newFrontier levels with
                        | None ->
                            stop <- true
                            newFrontier.Dispose queue
                        | Some f ->
                            frontier <- ClVector.Sparse f
                            newFrontier.Dispose queue

                | _ -> failwith "Not implemented"

            levels


    let singleSourcePushPull
        (add: Expr<bool option -> bool option -> bool option>)
        (mul: Expr<bool option -> bool option -> bool option>)
        (clContext: ClContext)
        workGroupSize
        =

        let SPARSITY = 0.001f

        let push nnz size =
            (float32 nnz) / (float32 size) <= SPARSITY

        let spMVTo =
            SpMV.runTo add mul clContext workGroupSize

        let spMSpV =
            SpMSpV.runBoolStandard add mul clContext workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let ofList = Vector.ofList clContext workGroupSize

        let maskComplementedTo =
            Vector.map2InPlace Mask.complementedOp clContext workGroupSize

        let maskComplemented =
            Vector.Sparse.Vector.map2SparseDense Mask.complementedOp clContext workGroupSize

        let fillSubVectorDenseTo =
            Vector.assignByMaskInPlace (Convert.assignToOption Mask.assign) clContext workGroupSize

        let fillSubVectorSparseTo =
            Vector.assignBySparseMaskInPlace (Convert.assignToOption Mask.assign) clContext workGroupSize

        let toSparse = Vector.toSparse clContext workGroupSize

        let toDense = Vector.toDense clContext workGroupSize

        let countNNZ =
            ClArray.count Predicates.isSome clContext workGroupSize

        fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix.CSR<bool>) (source: int) ->
            let vertexCount = matrix.RowCount

            let levels = zeroCreate queue HostInterop vertexCount

            let mutable frontier =
                ofList queue DeviceOnly Sparse vertexCount [ source, true ]

            let mutable level = 0
            let mutable stop = false

            while not stop do
                level <- level + 1

                match frontier with
                | ClVector.Sparse front ->
                    //Assigning new level values
                    fillSubVectorSparseTo queue levels front (clContext.CreateClCell level) levels

                    //Getting new frontier
                    match spMSpV queue matrix front with
                    | None ->
                        frontier.Dispose queue
                        stop <- true
                    | Some newFrontier ->
                        frontier.Dispose queue
                        //Filtering visited vertices
                        match maskComplemented queue DeviceOnly newFrontier levels with
                        | None ->
                            stop <- true
                            newFrontier.Dispose queue
                        | Some f ->
                            newFrontier.Dispose queue

                            //Push/pull
                            if (push f.NNZ f.Size) then
                                frontier <- ClVector.Sparse f
                            else
                                frontier <- toDense queue DeviceOnly (ClVector.Sparse f)
                                f.Dispose queue
                | ClVector.Dense front ->
                    //Assigning new level values
                    fillSubVectorDenseTo queue levels front (clContext.CreateClCell level) levels

                    //Getting new frontier
                    spMVTo queue matrix front front

                    maskComplementedTo queue front levels front

                    //Emptiness check
                    let NNZ = countNNZ queue front

                    stop <- NNZ = 0

                    //Push/pull
                    if not stop then
                        if (push NNZ front.Length) then
                            frontier <- ClVector.Sparse(toSparse queue DeviceOnly front)
                            front.Free queue
                    else
                        frontier.Dispose queue

            levels
