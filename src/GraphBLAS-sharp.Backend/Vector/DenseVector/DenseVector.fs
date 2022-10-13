namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module DenseVector =
    let zeroCreate (clContext: ClContext) (workGroupSize: int)  =
        let zeroCreate = ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (length: int) ->
            let resultValues = zeroCreate processor length

            resultValues :?> ClDenseVector<'a>

    let ofList (clContext: ClContext) (workGroupSize: int) (elements: (int * 'a) list) =
        let indices, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let toOptionArray = ClArray.toOptionArray clContext workGroupSize

        fun (processor: MailboxProcessor<_>) ->
            let values = clContext.CreateClArray values
            let indices = clContext.CreateClArray indices

            toOptionArray processor values indices elements.Length :?> ClDenseVector<'a>


    let copy (clContext: ClContext) (workGroupSize: int) =
        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClDenseVector<'a>) ->
            copy processor vector :?> ClDenseVector<'a>

    let elementWiseAddAtLeasOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) (workGroupSize: int) =

        let eWiseAdd =
            <@ fun (ndRange: Range1D) length (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

                 let gid = ndRange.GlobalID0

                 if gid < length then

                     let leftItem = leftVector[gid]
                     let rightItem = rightVector[gid]

                     match leftItem, rightItem with
                            | Some left, Some right ->
                                resultVector.[gid] <- (%opAdd) (Both (left, right))
                            | Some left, None ->
                                resultVector.[gid] <- (%opAdd) (Left left)
                            | None, Some right ->
                                resultVector.[gid] <- (%opAdd) (Right right)
                            | None, None ->
                                resultVector.[gid] <- None @>

        let kernel = clContext.Compile(eWiseAdd)

        fun (processor: MailboxProcessor<_>) (leftVector: ClDenseVector<'a>) (rightVector: ClDenseVector<'b>) ->

            let resultVector =
                clContext.CreateClArray(
                    leftVector.Size,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let ndRange = Range1D.CreateValid (leftVector.Size, workGroupSize)

            let kernel = kernel.GetKernel ()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            leftVector.Size
                            leftVector
                            rightVector
                            resultVector)
                )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultVector
