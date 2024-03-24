namespace GraphBLAS.FSharp

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ClCellExtensions

[<RequireQualifiedAccess>]
module ClArray =
    /// <summary>
    /// Creates an array given the dimension and a generator function to compute the elements.
    /// </summary>
    /// <param name="initializer">The function to generate the initial values for each index.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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

    /// <summary>
    /// Creates an array whose elements are all initially the given value.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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
            value.Free processor

            outputArray

    /// <summary>
    /// Creates an array where the entries are initially the default value of desired type.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let zeroCreate<'a> (clContext: ClContext) workGroupSize =

        let create = create clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode length ->
            create processor allocationMode length Unchecked.defaultof<'a>

    /// <summary>
    /// Builds a new array that contains the elements of the given array.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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

    /// <summary>
    /// Copies all elements from source to destination array.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let copyTo (clContext: ClContext) workGroupSize =
        let copy =
            <@ fun (ndRange: Range1D) (source: ClArray<'a>) (destination: ClArray<'a>) inputArrayLength ->

                let i = ndRange.GlobalID0

                if i < inputArrayLength then
                    destination.[i] <- source.[i] @>

        let program = clContext.Compile(copy)

        fun (processor: MailboxProcessor<_>) (source: ClArray<'a>) (destination: ClArray<'a>) ->
            if source.Length <> destination.Length then
                failwith "The source array length differs from the destination array length."

            let ndRange =
                Range1D.CreateValid(source.Length, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange source destination source.Length))

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    /// <summary>
    /// Creates an array of the given size by replicating the values of the given initial array.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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

    /// <summary>
    /// Removes duplicates form the given array.
    /// </summary>
    /// <param name="clContext">Computational context</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    /// <param name="inputArray">Should be sorted.</param>
    let removeDuplications (clContext: ClContext) workGroupSize =

        let scatter =
            Scatter.lastOccurrence clContext workGroupSize

        let getUniqueBitmap =
            Bitmap.lastOccurrence clContext workGroupSize

        let prefixSumExclude =
            ScanInternal.standardExcludeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let bitmap =
                getUniqueBitmap processor DeviceOnly inputArray

            let resultLength =
                (prefixSumExclude processor bitmap)
                    .ToHostAndFree(processor)

            let outputArray =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

            scatter processor bitmap inputArray outputArray

            bitmap.Free processor

            outputArray

    /// <summary>
    /// Tests if any element of the array satisfies the given predicate.
    /// </summary>
    /// <param name="predicate">The function to test the input elements.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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

    /// <summary>
    /// Maps every value from the given value array and, if the result of applying function is <c>Some</c>,
    /// places the result to a specific position from the input array of positions.
    /// If the result of mapping is <c>None</c>, it is just ignored.
    /// </summary>
    /// <param name="op">Function that maps elements from value array.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let private assignOption (op: Expr<'a -> 'b option>) (clContext: ClContext) workGroupSize =

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
                failwith "Lengths must be the same"

            let ndRange =
                Range1D.CreateValid(values.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange values.Length values positions result result.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    /// <summary>
    /// Applies the given function to each element of the array.
    /// Returns the array comprised of the results <c>x</c>
    /// for each element where the function returns <c>Some(x)</c>.
    /// </summary>
    /// <param name="predicate">The function to generate options from the elements.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let choose<'a, 'b> (predicate: Expr<'a -> 'b option>) (clContext: ClContext) workGroupSize =
        let getBitmap =
            Map.map<'a, int> (Map.chooseBitmap predicate) clContext workGroupSize

        let prefixSum =
            ScanInternal.standardExcludeInPlace clContext workGroupSize

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

    /// <summary>
    /// Maps pair of values from the given value arrays and, if the result of applying function is <c>Some</c>,
    /// places the result to a specific position from the input array of positions.
    /// If the result of mapping is <c>None</c>, it is just ignored.
    /// </summary>
    /// <param name="op">Function that maps pairs of elements from value arrays.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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
                failwith "Lengths must be the same"

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

    /// <summary>
    /// Applies the given function to each pair of elements of the two given arrays.
    /// Returns the array comprised of the results <c>x</c>
    /// for each pairs where the function returns <c>Some(x)</c>.
    /// </summary>
    /// <param name="predicate">The function to generate options from the pairs of elements.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let choose2 (predicate: Expr<'a -> 'b -> 'c option>) (clContext: ClContext) workGroupSize =
        let getBitmap =
            Map.map2<'a, 'b, int> (Map.choose2Bitmap predicate) clContext workGroupSize

        let prefixSum =
            ScanInternal.standardExcludeInPlace clContext workGroupSize

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

    /// <summary>
    /// Builds a new array that contains the given subrange specified by starting index and length.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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
    /// Divides lazily the input array into chunks of size at most chunkSize.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let chunkBySize (clContext: ClContext) workGroupSize =

        let chunkBySizeLazy = lazyChunkBySize clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode chunkSize (sourceArray: ClArray<'a>) ->
            chunkBySizeLazy processor allocationMode chunkSize sourceArray
            |> Seq.map (fun lazyValue -> lazyValue.Value)
            |> Seq.toArray

    /// <summary>
    /// Reads a range of elements from the first array and write them into the second.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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

    /// <summary>
    /// Builds a new array that contains the elements of each of the given sequence of arrays.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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

    /// <summary>
    /// Fills a range of elements of the array with the given value.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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

    /// <summary>
    /// Returns an array of each element in the input array and its predecessor,
    /// with the exception of the first element which is only returned as the predecessor of the second element.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let pairwise (clContext: ClContext) workGroupSize =

        let idGather =
            Gather.runInit Map.id clContext workGroupSize

        let incGather =
            Gather.runInit Map.inc clContext workGroupSize

        let map =
            Map.map2 <@ fun first second -> (first, second) @> clContext workGroupSize

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
        (lowerBound: Expr<int -> 'a -> ClArray<'a> -> 'b>)
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

    /// <summary>
    /// Finds the position of the largest value and the value itself
    /// that is less than the given one.
    /// </summary>
    /// <remarks>
    /// Array of values should be sorted.
    /// </remarks>
    /// <param name="clContext">OpenCL context.</param>
    let upperBoundAndValue<'a when 'a: comparison> clContext =
        bound<'a, int * 'a> Search.Bin.lowerBoundAndValue clContext

    /// <summary>
    /// Finds the position of the largest value that is less than the given one.
    /// </summary>
    /// <remarks>
    /// Array of values should be sorted.
    /// </remarks>
    /// <param name="clContext">OpenCL context.</param>
    let upperBound<'a when 'a: comparison> clContext =
        bound<'a, int> Search.Bin.lowerBound clContext

    /// <summary>
    /// Gets the value at the specified position from the input array.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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

    /// <summary>
    /// Sets an element of an array.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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

    let count<'a> (predicate: Expr<'a -> bool>) (clContext: ClContext) workGroupSize =

        let sum =
            Reduce.reduce <@ (+) @> clContext workGroupSize

        let getBitmap =
            Map.map<'a, int> (Map.predicateBitmap predicate) clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (array: ClArray<'a>) ->

            let bitmap = getBitmap processor DeviceOnly array

            let result =
                (sum processor bitmap).ToHostAndFree processor

            bitmap.Free processor

            result

    /// <summary>
    /// Builds a new array whose elements are the results of applying the given function
    /// to each of the elements of the array.
    /// </summary>
    /// <param name="op">The function to transform elements of the array.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let map<'a, 'b> (op: Expr<'a -> 'b>) (clContext: ClContext) workGroupSize = Map.map op clContext workGroupSize

    /// <summary>
    /// Builds a new array whose elements are the results of applying the given function
    /// to each of the elements of the array.
    /// </summary>
    /// <param name="op">The function to transform elements of the array.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let mapInPlace<'a> (op: Expr<'a -> 'a>) (clContext: ClContext) workGroupSize =
        Map.mapInPlace op clContext workGroupSize

    /// <summary>
    /// Builds a new array whose elements are the results of applying the given function
    /// to the corresponding pairs of values, where the first element of pair is from the given array
    /// and the second element is the given value.
    /// </summary>
    /// <param name="op">The function to transform elements of the array.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let mapWithValue<'a, 'b, 'c> (clContext: ClContext) workGroupSize (op: Expr<'a -> 'b -> 'c>) =
        Map.mapWithValue clContext workGroupSize op

    /// <summary>
    /// Builds a new array whose elements are the results of applying the given function
    /// to the corresponding elements of the two given arrays pairwise.
    /// </summary>
    /// <remarks>
    /// The two input arrays must have the same lengths.
    /// </remarks>
    /// <param name="map">The function to transform the pairs of the input elements.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let map2<'a, 'b, 'c> (map: Expr<'a -> 'b -> 'c>) (clContext: ClContext) workGroupSize =
        Map.map2 map clContext workGroupSize

    /// <summary>
    /// Fills the third given array with the results of applying the given function
    /// to the corresponding elements of the first two given arrays pairwise.
    /// </summary>
    /// <remarks>
    /// The first two input arrays must have the same lengths.
    /// </remarks>
    /// <param name="map">The function to transform the pairs of the input elements.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let map2InPlace<'a, 'b, 'c> (map: Expr<'a -> 'b -> 'c>) (clContext: ClContext) workGroupSize =
        Map.map2InPlace map clContext workGroupSize

    /// <summary>
    /// Excludes elements, pointed by the bitmap.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let excludeElements (clContext: ClContext) workGroupSize =

        let invert =
            mapInPlace ArithmeticOperations.intNotQ clContext workGroupSize

        let prefixSum =
            ScanInternal.standardExcludeInPlace clContext workGroupSize

        let scatter =
            Scatter.lastOccurrence clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (excludeBitmap: ClArray<int>) (inputArray: ClArray<'a>) ->

            invert queue excludeBitmap

            let length =
                (prefixSum queue excludeBitmap)
                    .ToHostAndFree queue

            if length = 0 then
                None
            else
                let result =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, length)

                scatter queue excludeBitmap inputArray result

                Some result
