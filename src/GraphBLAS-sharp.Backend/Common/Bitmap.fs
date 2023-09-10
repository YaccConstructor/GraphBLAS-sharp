namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

module Bitmap =
    let private getUniqueBitmapGeneral predicate (clContext: ClContext) workGroupSize =

        let getUniqueBitmap =
            <@ fun (ndRange: Range1D) (inputArray: ClArray<'a>) inputLength (isUniqueBitmap: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < inputLength then
                    let isUnique = (%predicate) gid inputLength inputArray // brahma error

                    if isUnique then
                        isUniqueBitmap.[gid] <- 1
                    else
                        isUniqueBitmap.[gid] <- 0 @>

        let kernel = clContext.Compile(getUniqueBitmap)

        fun (processor: MailboxProcessor<_>) allocationMode (inputArray: ClArray<'a>) ->

            let inputLength = inputArray.Length

            let ndRange =
                Range1D.CreateValid(inputLength, workGroupSize)

            let bitmap =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, inputLength)

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray inputLength bitmap))

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            bitmap

    let firstOccurrence clContext =
        getUniqueBitmapGeneral
        <| Predicates.firstOccurrence ()
        <| clContext

    let lastOccurrence clContext =
        getUniqueBitmapGeneral
        <| Predicates.lastOccurrence ()
        <| clContext

    let private getUniqueBitmap2General<'a when 'a: equality> getUniqueBitmap (clContext: ClContext) workGroupSize =

        let map =
            Map.map2 <@ fun x y -> x ||| y @> clContext workGroupSize

        let firstGetBitmap = getUniqueBitmap clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (firstArray: ClArray<'a>) (secondArray: ClArray<'a>) ->
            let firstBitmap =
                firstGetBitmap processor DeviceOnly firstArray

            let secondBitmap =
                firstGetBitmap processor DeviceOnly secondArray

            let result =
                map processor allocationMode firstBitmap secondBitmap

            firstBitmap.Free processor
            secondBitmap.Free processor

            result

    let firstOccurrence2 clContext =
        getUniqueBitmap2General firstOccurrence clContext

    let lastOccurrence2 clContext =
        getUniqueBitmap2General lastOccurrence clContext
