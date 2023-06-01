namespace GraphBLAS.FSharp.Backend.Common

open System.Collections.Generic
open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Quotes

module ClArray =
    let init (initializer: Expr<int -> 'a>) (clContext: ClContext) workGroupSize =

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

    let map<'a, 'b> (op: Expr<'a -> 'b>) (clContext: ClContext) workGroupSize =

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

    let mapWithValue<'a, 'b, 'c> (clContext: ClContext) workGroupSize (op: Expr<'a -> 'b -> 'c>) =

        let map =
            <@ fun (ndRange: Range1D) lenght (value: ClCell<'a>) (inputArray: ClArray<'b>) (result: ClArray<'c>) ->

                let gid = ndRange.GlobalID0

                if gid < lenght then
                    result.[gid] <- (%op) value.Value inputArray.[gid] @>

        let kernel = clContext.Compile map

        fun (processor: MailboxProcessor<_>) allocationMode (value: 'a) (inputArray: ClArray<'b>) ->

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, inputArray.Length)

            let valueClCell = value |> clContext.CreateClCell

            let ndRange =
                Range1D.CreateValid(inputArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray.Length valueClCell inputArray result)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            result

    let map2InPlace<'a, 'b, 'c> (map: Expr<'a -> 'b -> 'c>) (clContext: ClContext) workGroupSize =

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

    let map2<'a, 'b, 'c> map (clContext: ClContext) workGroupSize =
        let map2 =
            map2InPlace<'a, 'b, 'c> map clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftArray: ClArray<'a>) (rightArray: ClArray<'b>) ->

            let resultArray =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, leftArray.Length)

            map2 processor leftArray rightArray resultArray

            resultArray

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
                map2 <@ fun x y -> x ||| y @> clContext workGroupSize

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

    ///<description>Remove duplicates form the given array.</description>
    ///<param name="clContext">Computational context</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    ///<param name="inputArray">Should be sorted.</param>
    let distinct (clContext: ClContext) workGroupSize =

        let scatter =
            Scatter.lastOccurrence clContext workGroupSize

        let getUniqueBitmap =
            Bitmap.lastOccurrence clContext workGroupSize

        let prefixSumExclude =
            PrefixSum.standardExcludeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let bitmap =
                getUniqueBitmap processor DeviceOnly inputArray

            let resultLength =
                (prefixSumExclude processor bitmap)
                    .ToHostAndFree(processor)

            let outputArray =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

            scatter processor bitmap inputArray outputArray

            processor.Post <| Msg.CreateFreeMsg<_>(bitmap)

            outputArray

    let exists (predicate: Expr<'a -> bool>) (clContext: ClContext) workGroupSize =

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

    let assignOption (op: Expr<'a -> 'b option>) (clContext: ClContext) workGroupSize =

        let assign =
            <@ fun (ndRange: Range1D) length (values: ClArray<'a>) (positions: ClArray<int>) (result: ClArray<'b>) resultLength ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    let position = positions.[gid]
                    let value = values.[gid]

                    // seems like scatter (option scatter) ???
                    if 0 <= position && position < resultLength then
                        match (%op) value with
                        | Some value -> result.[position] <- value
                        | None -> () @>

        let kernel = clContext.Compile assign

        fun (processor: MailboxProcessor<_>) (values: ClArray<'a>) (positions: ClArray<int>) (result: ClArray<'b>) ->

            if values.Length <> positions.Length then
                failwith "lengths must be the same"

            let ndRange =
                Range1D.CreateValid(values.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange values.Length values positions result result.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let choose<'a, 'b> (predicate: Expr<'a -> 'b option>) (clContext: ClContext) workGroupSize =
        let getBitmap =
            map<'a, int> (Map.chooseBitmap predicate) clContext workGroupSize

        let prefixSum =
            PrefixSum.standardExcludeInPlace clContext workGroupSize

        let assignValues =
            assignOption predicate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (sourceValues: ClArray<'a>) ->

            let positions =
                getBitmap processor DeviceOnly sourceValues

            let resultLength =
                (prefixSum processor positions)
                    .ToHostAndFree(processor)

            if resultLength = 0 then
                positions.Free processor

                None
            else
                let result =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                assignValues processor sourceValues positions result

                positions.Free processor

                Some result

    let assignOption2 (op: Expr<'a -> 'b -> 'c option>) (clContext: ClContext) workGroupSize =

        let assign =
            <@ fun (ndRange: Range1D) length (firstValues: ClArray<'a>) (secondValues: ClArray<'b>) (positions: ClArray<int>) (result: ClArray<'c>) resultLength ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    let position = positions.[gid]

                    let leftValue = firstValues.[gid]
                    let rightValue = secondValues.[gid]

                    // seems like scatter2 (option scatter2) ???
                    if 0 <= position && position < resultLength then
                        match (%op) leftValue rightValue with
                        | Some value -> result.[position] <- value
                        | None -> () @>

        let kernel = clContext.Compile assign

        fun (processor: MailboxProcessor<_>) (firstValues: ClArray<'a>) (secondValues: ClArray<'b>) (positions: ClArray<int>) (result: ClArray<'c>) ->

            if firstValues.Length <> secondValues.Length
               || secondValues.Length <> positions.Length then
                failwith "lengths must be the same"

            let ndRange =
                Range1D.CreateValid(firstValues.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            firstValues.Length
                            firstValues
                            secondValues
                            positions
                            result
                            result.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let choose2 (predicate: Expr<'a -> 'b -> 'c option>) (clContext: ClContext) workGroupSize =
        let getBitmap =
            map2<'a, 'b, int> (Map.choose2Bitmap predicate) clContext workGroupSize

        let prefixSum =
            PrefixSum.standardExcludeInPlace clContext workGroupSize

        let assignValues =
            assignOption2 predicate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (firstValues: ClArray<'a>) (secondValues: ClArray<'b>) ->

            let positions =
                getBitmap processor DeviceOnly firstValues secondValues

            let resultLength =
                (prefixSum processor positions)
                    .ToHostAndFree(processor)

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            assignValues processor firstValues secondValues positions result

            result

    let sub (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) startIndex count (sourceArray: ClArray<'a>) (targetChunk: ClArray<'a>) ->

                let gid = ndRange.GlobalID0

                if gid < count then
                    let sourcePosition = gid + startIndex

                    targetChunk.[gid] <- sourceArray.[sourcePosition] @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) allocationMode (sourceArray: ClArray<'a>) startIndex count ->
            if count <= 0 then
                failwith "Count must be greater than zero"

            if startIndex < 0 then
                failwith "startIndex must be greater then zero"

            if startIndex + count > sourceArray.Length then
                failwith "startIndex and count sum is larger than the size of the array"

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, count)

            let ndRange =
                Range1D.CreateValid(count, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange startIndex count sourceArray result))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            result

    /// <summary>
    /// Lazy divides the input array into chunks of size at most chunkSize.
    /// </summary>
    /// <param name="clContext">Cl context.</param>
    /// <param name="workGroupSize">Work group size.</param>
    /// <remarks>
    /// Since calculations are performed lazily, the array should not change.
    /// </remarks>
    let lazyChunkBySize (clContext: ClContext) workGroupSize =

        let sub = sub clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode chunkSize (sourceArray: ClArray<'a>) ->
            if chunkSize <= 0 then
                failwith "The size of the chunk cannot be less than 1"

            let chunkCount = (sourceArray.Length - 1) / chunkSize + 1

            let sub = sub processor allocationMode sourceArray

            seq {
                for i in 0 .. chunkCount - 1 do
                    let startIndex = i * chunkSize

                    let count =
                        min chunkSize (sourceArray.Length - startIndex)

                    yield lazy (sub startIndex count)
            }

    /// <summary>
    /// Divides the input array into chunks of size at most chunkSize.
    /// </summary>
    /// <param name="clContext">Cl context.</param>
    /// <param name="workGroupSize">Work group size.</param>
    let chunkBySize (clContext: ClContext) workGroupSize =

        let chunkBySizeLazy = lazyChunkBySize clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode chunkSize (sourceArray: ClArray<'a>) ->
            chunkBySizeLazy processor allocationMode chunkSize sourceArray
            |> Seq.map (fun lazyValue -> lazyValue.Value)
            |> Seq.toArray

    let blit<'a> (clContext: ClContext) workGroupSize =

        let assign =
            <@ fun (ndRange: Range1D) sourceIndex (sourceArray: ClArray<'a>) (targetArray: ClArray<'a>) targetPosition count ->

                let gid = ndRange.GlobalID0

                if gid < count then
                    let readPosition = gid + sourceIndex
                    let writePosition = gid + targetPosition

                    targetArray.[writePosition] <- sourceArray.[readPosition] @>

        let kernel = clContext.Compile assign

        fun (processor: MailboxProcessor<_>) (sourceArray: ClArray<'a>) sourceIndex (targetArray: ClArray<'a>) targetIndex count ->
            if count = 0 then
                // nothing to do
                ()
            else
                if count < 0 then
                    failwith "Count must be greater than zero"

                if sourceIndex < 0
                   && sourceIndex + count >= sourceArray.Length then
                    failwith "The source index does not match"

                if targetIndex < 0
                   && targetIndex + count >= targetArray.Length then
                    failwith "The target index does not match"

                let ndRange =
                    Range1D.CreateValid(targetArray.Length, workGroupSize)

                let kernel = kernel.GetKernel()

                processor.Post(
                    Msg.MsgSetArguments
                        (fun () -> kernel.KernelFunc ndRange sourceIndex sourceArray targetArray targetIndex count)
                )

                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let concat (clContext: ClContext) workGroupSize =

        let blit = blit clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (sourceArrays: ClArray<'a> seq) ->

            let resultLength =
                sourceArrays
                |> Seq.sumBy (fun array -> array.Length)

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            // write each array to result
            Seq.fold
                (fun previousLength (array: ClArray<_>) ->
                    blit processor array 0 result previousLength array.Length
                    previousLength + array.Length)
                0
                sourceArrays
            |> ignore

            result

    let fill (clContext: ClContext) workGroupSize =

        let fill =
            <@ fun (ndRange: Range1D) firstPosition count (value: ClCell<'a>) (targetArray: ClArray<'a>) ->

                let gid = ndRange.GlobalID0
                let writePosition = gid + firstPosition

                if gid < count then
                    targetArray.[writePosition] <- value.Value @>

        let kernel = clContext.Compile fill

        fun (processor: MailboxProcessor<_>) value firstPosition count (targetArray: ClArray<'a>) ->
            if count = 0 then
                ()
            else
                if count < 0 then
                    failwith "Count must be greater than zero"

                if firstPosition + count > targetArray.Length then
                    failwith "The array should fit completely"

                let ndRange =
                    Range1D.CreateValid(count, workGroupSize)

                let kernel = kernel.GetKernel()

                processor.Post(
                    Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange firstPosition count value targetArray)
                )

                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let pairwise (clContext: ClContext) workGroupSize =

        let idGather =
            Gather.runInit Map.id clContext workGroupSize

        let incGather =
            Gather.runInit Map.inc clContext workGroupSize

        let map =
            map2 <@ fun first second -> (first, second) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (values: ClArray<'a>) ->
            if values.Length > 1 then
                let resultLength = values.Length - 1

                let firstItems =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                idGather processor values firstItems

                let secondItems =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                incGather processor values secondItems

                let result =
                    map processor allocationMode firstItems secondItems

                firstItems.Free processor
                secondItems.Free processor

                Some result
            else
                None

    let private bound<'a, 'b when 'a: equality and 'a: comparison>
        (lowerBound: Expr<(int -> 'a -> ClArray<'a> -> 'b)>)
        (clContext: ClContext)
        workGroupSize
        =

        let kernel =
            <@ fun (ndRange: Range1D) length (values: ClArray<'a>) (value: ClCell<'a>) (result: ClCell<'b>) ->

                let value = value.Value
                let gid = ndRange.GlobalID0

                if gid = 0 then

                    result.Value <- (%lowerBound) length value values @>

        let program = clContext.Compile(kernel)

        fun (processor: MailboxProcessor<_>) (values: ClArray<'a>) (value: ClCell<'a>) ->
            let result =
                clContext.CreateClCell Unchecked.defaultof<'b>

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(1, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange values.Length values value result))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            result

    let upperBoundAndValue<'a when 'a: comparison> clContext =
        bound<'a, int * 'a> Search.Bin.lowerBoundAndValue clContext

    let upperBound<'a when 'a: comparison> clContext =
        bound<'a, int> Search.Bin.lowerBound clContext

    let item<'a> (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) index (array: ClArray<'a>) (result: ClCell<'a>) ->

                let gid = ndRange.GlobalID0

                if gid = 0 then
                    result.Value <- array.[index] @>

        let program = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (index: int) (array: ClArray<'a>) ->

            if index < 0 || index >= array.Length then
                failwith "Index out of range"

            let result =
                clContext.CreateClCell Unchecked.defaultof<'a>

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(1, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange index array result))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            result

    let set<'a> (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) index (array: ClArray<'a>) (value: ClCell<'a>) ->

                let gid = ndRange.GlobalID0

                if gid = 0 then
                    array.[index] <- value.Value @>

        let program = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (array: ClArray<'a>) (index: int) (value: 'a) ->

            if index < 0 || index >= array.Length then
                failwith "Index out of range"

            let value = clContext.CreateClCell value

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(1, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange index array value))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)
