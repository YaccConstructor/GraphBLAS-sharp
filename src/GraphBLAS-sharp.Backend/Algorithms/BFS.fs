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
open GraphBLAS.FSharp.Backend.Vector.Sparse

module BFS =
    let singleSource
        (clContext: ClContext)
        (add: Expr<int option -> int option -> int option>)
        (mul: Expr<'a option -> int option -> int option>)
        workGroupSize
        =

        let spMVTo =
            SpMV.runTo clContext add mul workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let ofList = Vector.ofList clContext workGroupSize

        let maskComplementedTo =
            DenseVector.map2Inplace clContext Mask.complementedOp workGroupSize

        let fillSubVectorTo =
            DenseVector.assignByMaskInplace clContext (Convert.assignToOption Mask.assign) workGroupSize

        let containsNonZero =
            ClArray.exists clContext workGroupSize Predicates.isSome

        fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix.CSR<'a>) (source: int) ->
            let vertexCount = matrix.RowCount

            let levels = zeroCreate queue DeviceOnly vertexCount

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

                front.Dispose queue

                levels
            | _ -> failwith "Not implemented"


    let singleSourceSparse
        (clContext: ClContext)
        (add: Expr<bool option -> bool option -> bool option>)
        (mul: Expr<bool option -> bool option -> bool option>)
        workGroupSize
        =

        let spMSpV =
            SpMSpV.run clContext add mul workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let ofList = Vector.ofList clContext workGroupSize

        let maskComplemented =
            SparseVector.map2SparseDense clContext Mask.complementedOp workGroupSize

        let fillSubVectorTo =
            DenseVector.assignBySparseMaskInplace clContext (Convert.assignToOption Mask.assign) workGroupSize

        let containsNonZero =
            SparseVector.exists clContext workGroupSize (Predicates.notEquals false)

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
                    let newFrontier = spMSpV queue matrix front

                    frontier.Dispose queue

                    frontier <- ClVector.Sparse(maskComplemented queue DeviceOnly newFrontier levels)

                    newFrontier.Dispose queue

                    //Checking if front is empty
                    match frontier with
                    | ClVector.Sparse front ->
                        stop <-
                            not
                            <| (containsNonZero queue front).ToHostAndFree queue
                    | _ -> ()

                | _ -> failwith "Not implemented"

            frontier.Dispose queue

            levels

    let singleSourcePushPull
        (clContext: ClContext)
        (add: Expr<bool option -> bool option -> bool option>)
        (mul: Expr<bool option -> bool option -> bool option>)
        workGroupSize
        =

        let SPARSITY = 0.001f

        let spMVTo =
            SpMV.runTo clContext add mul workGroupSize

        let spMSpV =
            SpMSpV.run clContext add mul workGroupSize

        let zeroCreate =
            ClArray.zeroCreate clContext workGroupSize

        let ofList = Vector.ofList clContext workGroupSize

        let maskComplementedTo =
            DenseVector.map2Inplace clContext Mask.complementedOp workGroupSize

        let maskComplemented =
            SparseVector.map2SparseDense clContext Mask.complementedOp workGroupSize

        let fillSubVectorDenseTo =
            DenseVector.assignByMaskInplace clContext (Convert.assignToOption Mask.assign) workGroupSize

        let fillSubVectorSparseTo =
            DenseVector.assignBySparseMaskInplace clContext (Convert.assignToOption Mask.assign) workGroupSize

        let containsNonZeroSparse =
            SparseVector.exists clContext workGroupSize (Predicates.notEquals false)

        let toSparse =
            DenseVector.toSparse clContext workGroupSize

        let toDense =
            SparseVector.toDense clContext workGroupSize

        let countNNZ =
            ClArray.count clContext workGroupSize Predicates.isSome

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
                    let newFrontier = spMSpV queue matrix front

                    frontier.Dispose queue

                    let newMaskedFrontier =
                        maskComplemented queue DeviceOnly newFrontier levels

                    newFrontier.Dispose queue

                    //Checking if front is empty
                    stop <-
                        not
                        <| (containsNonZeroSparse queue newMaskedFrontier)
                            .ToHostAndFree queue

                    if not stop then
                        //Push/pull
                        if ((float32 newMaskedFrontier.NNZ)
                            / (float32 newMaskedFrontier.Size)
                            <= SPARSITY) then
                            frontier <- ClVector.Sparse newMaskedFrontier
                        else
                            printfn "Sparse to dense, front size %i" (newMaskedFrontier.NNZ)
                            frontier <- ClVector.Dense(toDense queue DeviceOnly newMaskedFrontier)
                            newMaskedFrontier.Dispose queue

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
                        if ((float32 NNZ) / (float32 front.Length) <= SPARSITY) then
                            printfn "Dense to sparse,  front size %i" NNZ
                            frontier <- ClVector.Sparse(toSparse queue DeviceOnly front)
                            front.Dispose queue

            frontier.Dispose queue

            levels
