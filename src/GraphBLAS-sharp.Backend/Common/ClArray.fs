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
        <| Predicates.firstOccurrence ()
        <| clContext

    let getUniqueBitmapLastOccurrence clContext =
        getUniqueBitmapGeneral
        <| Predicates.lastOccurrence ()
        <| clContext

    ///<description>Remove duplicates form the given array.</description>
    ///<param name="clContext">Computational context</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    ///<param name="inputArray">Should be sorted.</param>
    let removeDuplications (clContext: ClContext) workGroupSize =

        let scatter =
            Scatter.lastOccurrence clContext workGroupSize

        let getUniqueBitmap =
            getUniqueBitmapLastOccurrence clContext workGroupSize

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

        let map =
            map2 clContext workGroupSize <@ fun x y -> x ||| y @>

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

    let getUniqueBitmap2FirstOccurrence clContext =
        getUniqueBitmap2General getUniqueBitmapFirstOccurrence clContext

    let getUniqueBitmap2LastOccurrence clContext =
        getUniqueBitmap2General getUniqueBitmapLastOccurrence clContext

    let assignOption (clContext: ClContext) workGroupSize (op: Expr<'a -> 'b option>) =

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

    let choose<'a, 'b> (clContext: ClContext) workGroupSize (predicate: Expr<'a -> 'b option>) =
        let getBitmap =
            map<'a, int> clContext workGroupSize
            <| Map.chooseBitmap predicate

        let prefixSum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let assignValues =
            assignOption clContext workGroupSize predicate

        fun (processor: MailboxProcessor<_>) allocationMode (sourceValues: ClArray<'a>) ->

            let positions =
                getBitmap processor DeviceOnly sourceValues

            let resultLength =
                (prefixSum processor positions)
                    .ToHostAndFree(processor)

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            assignValues processor sourceValues positions result

            result

    let assignOption2 (clContext: ClContext) workGroupSize (op: Expr<'a -> 'b -> 'c option>) =

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

    let choose2 (clContext: ClContext) workGroupSize (predicate: Expr<'a -> 'b -> 'c option>) =
        let getBitmap =
            map2<'a, 'b, int> clContext workGroupSize
            <| Map.choose2Bitmap predicate

        let prefixSum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let assignValues =
            assignOption2 clContext workGroupSize predicate

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

    let getChunk (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) startIndex endIndex (sourceArray: ClArray<'a>) (targetChunk: ClArray<'a>) ->

                let gid = ndRange.GlobalID0
                let sourcePosition = gid + startIndex

                if sourcePosition < endIndex then

                    targetChunk.[gid] <- sourceArray.[sourcePosition] @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) allocationMode (sourceArray: ClArray<'a>) startIndex endIndex ->
            if startIndex < 0 then
                failwith "startIndex is less than zero"

            if startIndex >= endIndex then
                failwith "startIndex is greater than or equal to the endIndex"

            if endIndex > sourceArray.Length then
                failwith "endIndex is larger than the size of the array"

            let resultLength = endIndex - startIndex

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            let ndRange =
                Range1D.CreateValid(resultLength, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange startIndex endIndex sourceArray result)
            )

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

        let getChunk = getChunk clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode chunkSize (sourceArray: ClArray<'a>) ->
            if chunkSize <= 0 then
                failwith "The size of the piece cannot be less than 1"

            let chunkCount = (sourceArray.Length - 1) / chunkSize

            let getChunk =
                getChunk processor allocationMode sourceArray

            seq {
                for i in 0 .. chunkCount do
                    let startIndex = i * chunkSize

                    let endIndex =
                        min (startIndex + chunkSize) sourceArray.Length

                    yield lazy (getChunk startIndex endIndex)
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

    let assign<'a> (clContext: ClContext) workGroupSize =

        let assign =
            <@ fun (ndRange: Range1D) targetPosition sourceArrayLength (sourceArray: ClArray<'a>) (targetArray: ClArray<'a>) ->

                let gid = ndRange.GlobalID0

                let resultPosition = gid + targetPosition

                if gid < sourceArrayLength then

                    targetArray.[resultPosition] <- sourceArray.[gid] @>

        let kernel = clContext.Compile assign

        fun (processor: MailboxProcessor<_>) (sourceArray: ClArray<'a>) targetPosition (targetArray: ClArray<'a>) ->
            if targetPosition < 0 then
                failwith "The starting position cannot be less than zero"

            if targetPosition + sourceArray.Length > targetArray.Length then
                failwith "The array should fit completely"

            let ndRange =
                Range1D.CreateValid(targetArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange targetPosition sourceArray.Length sourceArray targetArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let concat (clContext: ClContext) workGroupSize =

        let assign = assign clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (sourceArrays: ClArray<'a> seq) ->

            let resultLength =
                sourceArrays
                |> Seq.sumBy (fun array -> array.Length)

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

            // write each array to result
            Seq.fold
                (fun previousLength (array: ClArray<_>) ->
                    assign processor array previousLength result
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
                if firstPosition + count > targetArray.Length then
                    failwith "The array should fit completely"

                if firstPosition < 0 then
                    failwith "The starting position cannot be less than zero"

                if count < 0 then
                    failwith "The count cannot be less than zero"

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
            map2 clContext workGroupSize <@ fun first second -> (first, second) @>

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
