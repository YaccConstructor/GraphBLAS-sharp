namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

module ClArray =
    let init (clContext: ClContext) workGroupSize (initializer: Expr<int -> 'a>) =

        let init =
            <@ fun (range: Range1D) (outputBuffer: ClArray<'a>) (length: int) ->

                let i = range.GlobalID0

                if i < length then
                    outputBuffer.[i] <- (%initializer) i @>

        let program = clContext.Compile(init)

        fun (processor: MailboxProcessor<_>) allocationMode (length: int) ->
            let outputArray =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, length)

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(length, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange outputArray length))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    let create (clContext: ClContext) workGroupSize =

        let create =
            <@ fun (range: Range1D) (outputBuffer: ClArray<'a>) (length: int) (value: ClCell<'a>) ->

                let i = range.GlobalID0

                if i < length then
                    outputBuffer.[i] <- value.Value @>

        let program = clContext.Compile(create)

        fun (processor: MailboxProcessor<_>) allocationMode (length: int) (value: 'a) ->
            let value = clContext.CreateClCell(value)

            let outputArray =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, length)

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(length, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange outputArray length value))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)
            processor.Post(Msg.CreateFreeMsg(value))

            outputArray

    let zeroCreate<'a> (clContext: ClContext) workGroupSize =

        let create = create clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode length ->
            create processor allocationMode length Unchecked.defaultof<'a>

    let copy (clContext: ClContext) workGroupSize =
        let copy =
            <@ fun (ndRange: Range1D) (inputArrayBuffer: ClArray<'a>) (outputArrayBuffer: ClArray<'a>) inputArrayLength ->

                let i = ndRange.GlobalID0

                if i < inputArrayLength then
                    outputArrayBuffer.[i] <- inputArrayBuffer.[i] @>

        let program = clContext.Compile(copy)

        fun (processor: MailboxProcessor<_>) allocationMode (inputArray: ClArray<'a>) ->
            let ndRange =
                Range1D.CreateValid(inputArray.Length, workGroupSize)

            let outputArray =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, inputArray.Length)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray outputArray inputArray.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    let replicate (clContext: ClContext) workGroupSize =

        let replicate =
            <@ fun (ndRange: Range1D) (inputArrayBuffer: ClArray<'a>) (outputArrayBuffer: ClArray<'a>) inputArrayLength outputArrayLength ->

                let i = ndRange.GlobalID0

                if i < outputArrayLength then
                    outputArrayBuffer.[i] <- inputArrayBuffer.[i % inputArrayLength] @>

        let kernel = clContext.Compile(replicate)

        fun (processor: MailboxProcessor<_>) allocationMode (inputArray: ClArray<'a>) count ->
            let outputArrayLength = inputArray.Length * count

            let outputArray =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, outputArrayLength)

            let ndRange =
                Range1D.CreateValid(outputArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange inputArray outputArray inputArray.Length outputArrayLength)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

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

    let getUniqueBitmapFirstOccurrence clContext =
        getUniqueBitmapGeneral
        <| <@ fun (gid: int) (_: int) (inputArray: ClArray<'a>) ->
               gid = 0 || inputArray.[gid - 1] <> inputArray.[gid] @>
        <| clContext

    let getUniqueBitmapLastOccurrence clContext =
        getUniqueBitmapGeneral
        <| <@ fun (gid: int) (length: int) (inputArray: ClArray<'a>) ->
                gid = length - 1 || inputArray.[gid] <> inputArray.[gid + 1] @>
        <| clContext

    ///<description>Remove duplicates form the given array.</description>
    ///<param name="clContext">Computational context</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    ///<param name="inputArray">Should be sorted.</param>
    let removeDuplications (clContext: ClContext) workGroupSize =

        let scatter =
            Scatter.lastOccurrence clContext workGroupSize

        let getUniqueBitmap = getUniqueBitmapLastOccurrence clContext workGroupSize

        let prefixSumExclude =
            PrefixSum.runExcludeInplace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let bitmap =
                getUniqueBitmap processor DeviceOnly inputArray

            let resultLength =
                (prefixSumExclude processor bitmap 0)
                    .ToHostAndFree(processor)

            let outputArray =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

            scatter processor bitmap inputArray outputArray

            processor.Post <| Msg.CreateFreeMsg<_>(bitmap)

            outputArray

    let exists (clContext: ClContext) workGroupSize (predicate: Expr<'a -> bool>) =

        let exists =
            <@ fun (ndRange: Range1D) length (vector: ClArray<'a>) (result: ClCell<bool>) ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    let isExist = (%predicate) vector.[gid]

                    if isExist then result.Value <- true @>

        let kernel = clContext.Compile exists

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a>) ->

            let result = clContext.CreateClCell false

            let ndRange =
                Range1D.CreateValid(vector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange vector.Length vector result))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            result

    let map<'a, 'b> (clContext: ClContext) workGroupSize (op: Expr<'a -> 'b>) =

        let map =
            <@ fun (ndRange: Range1D) lenght (inputArray: ClArray<'a>) (result: ClArray<'b>) ->

                let gid = ndRange.GlobalID0

                if gid < lenght then
                    result.[gid] <- (%op) inputArray.[gid] @>

        let kernel = clContext.Compile map

        fun (processor: MailboxProcessor<_>) allocationMode (inputArray: ClArray<'a>) ->

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, inputArray.Length)

            let ndRange =
                Range1D.CreateValid(inputArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray.Length inputArray result))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            result

    let map2Inplace<'a, 'b, 'c> (clContext: ClContext) workGroupSize (map: Expr<'a -> 'b -> 'c>) =

        let kernel =
            <@ fun (ndRange: Range1D) length (leftArray: ClArray<'a>) (rightArray: ClArray<'b>) (resultArray: ClArray<'c>) ->

                let gid = ndRange.GlobalID0

                if gid < length then

                    resultArray.[gid] <- (%map) leftArray.[gid] rightArray.[gid] @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (leftArray: ClArray<'a>) (rightArray: ClArray<'b>) (resultArray: ClArray<'c>) ->

            let ndRange =
                Range1D.CreateValid(resultArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange resultArray.Length leftArray rightArray resultArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let map2<'a, 'b, 'c> (clContext: ClContext) workGroupSize map =
        let map2 =
            map2Inplace<'a, 'b, 'c> clContext workGroupSize map

        fun (processor: MailboxProcessor<_>) allocationMode (leftArray: ClArray<'a>) (rightArray: ClArray<'b>) ->

            let resultArray =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, leftArray.Length)

            map2 processor leftArray rightArray resultArray

            resultArray

    let getUniqueBitmap2General<'a when 'a: equality> getUniqueBitmap (clContext: ClContext) workGroupSize =

        let map = map2 clContext workGroupSize <@ fun x y -> x ||| y @>

        let firstGetBitmap = getUniqueBitmap clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (firstArray: ClArray<'a>) (secondArray: ClArray<'a>) ->
            let firstBitmap = firstGetBitmap processor DeviceOnly firstArray

            let secondBitmap = firstGetBitmap processor DeviceOnly secondArray

            let result = map processor allocationMode firstBitmap secondBitmap

            firstBitmap.Free processor
            secondBitmap.Free processor

            result

    let getUniqueBitmap2FirstOccurrence clContext =
        getUniqueBitmap2General getUniqueBitmapFirstOccurrence clContext

    let getUniqueBitmap2LastOccurrence clContext =
        getUniqueBitmap2General getUniqueBitmapLastOccurrence clContext

    let private assignOption (clContext: ClContext) workGroupSize (op: Expr<'a -> 'b option>) =

        let assign =
            <@ fun (ndRange: Range1D) length (values: ClArray<'a>) (positions: ClArray<int>) (result: ClArray<'b>) resultLength ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    let position = positions.[gid]
                    let value = values.[gid]

                    // seems like scatter (option scatter) ???
                    if 0 <= position && position < resultLength then
                        match (%op) value with
                        | Some value ->
                            result.[position] <- value
                        | None -> () @>

        let kernel = clContext.Compile assign

        fun (processor: MailboxProcessor<_>) (values: ClArray<'a>) (positions: ClArray<int>) (result: ClArray<'b>) ->

            let ndRange =
                Range1D.CreateValid(values.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange values.Length values positions result result.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let choose<'a, 'b> (clContext: ClContext) workGroupSize (predicate: Expr<'a -> 'b option>) =
        let getBitmap =
            map<'a, int> clContext workGroupSize
            <| Map.chooseBitmap predicate

        let prefixSum = PrefixSum.standardExcludeInplace clContext workGroupSize

        let assignValues = assignOption clContext workGroupSize predicate

        fun (processor: MailboxProcessor<_>) allocationMode (sourceValues: ClArray<'a>) ->

            let positions = getBitmap processor DeviceOnly sourceValues

            let resultLength =
                (prefixSum processor positions)
                    .ToHostAndFree(processor)

            let result = clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            assignValues processor sourceValues positions result

            result

