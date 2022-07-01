namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module ClArray =
    let init
        (initializer: Expr<int -> 'a>)
        (clContext: ClContext)
        workGroupSize =

        let init =
            <@
                fun (range: Range1D)
                    (outputBuffer: ClArray<'a>)
                    (length: int) ->

                    let i = range.GlobalID0
                    if i < length then
                        outputBuffer.[i] <- (%initializer) i
            @>
        let program = clContext.Compile(init)

        fun (processor: MailboxProcessor<_>)
            (length: int) ->

            let outputArray = clContext.CreateClArray(length)

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(length, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange outputArray length)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    let create
        (value: 'a)
        (clContext: ClContext)
        workGroupSize =

        let init = init <@ fun _ -> value @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (length: int) ->
            init processor length

    let zeroCreate
        (clContext: ClContext)
        workGroupSize =

        let create = create Unchecked.defaultof<'a> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (length: int) ->
            create processor length

    let zipWith
        (f: Expr<'a -> 'b -> 'c>)
        (clContext: ClContext)
        workGroupSize =

        let zipWith =
            <@
                fun (range: Range1D)
                    (firstBuffer: ClArray<'a>)
                    (secondBuffer: ClArray<'b>)
                    (length: int)
                    (outputBuffer: ClArray<_>) ->

                    let i = range.GlobalID0
                    if i < length then
                        let a = firstBuffer.[i]
                        let b = secondBuffer.[i]
                        outputBuffer.[i] <- (%f) a b
            @>
        let program = clContext.Compile(zipWith)

        fun (processor: MailboxProcessor<_>)
            (firstArray: ClArray<'a>)
            (secondArray: ClArray<'b>) ->

            let length = firstArray.Length
            if length <> secondArray.Length then
                invalidArg "secondArray" "Lengths of the input arrays must be equal"

            let outputArray = clContext.CreateClArray(length)

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(length, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange firstArray secondArray length outputArray)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    let unzipWith
        (f: Expr<'a -> struct('b * 'c)>)
        (clContext: ClContext)
        workGroupSize =

        let unzipWith =
            <@
                fun (range: Range1D)
                    (inputBuffer: ClArray<'a>)
                    (length: int)
                    (firstBuffer: ClArray<'b>)
                    (secondBuffer: ClArray<'c>) ->

                    let i = range.GlobalID0
                    if i < length then
                        let a = inputBuffer.[i]
                        let struct(b, c) = (%f) a
                        firstBuffer.[i] <- b
                        secondBuffer.[i] <- c
            @>
        let program = clContext.Compile(unzipWith)

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<'a>) ->

            let length = inputArray.Length

            let firstOutputArray = clContext.CreateClArray(length)
            let secondOutputArray = clContext.CreateClArray(length)

            let ndRange = Range1D.CreateValid(length, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray length firstOutputArray secondOutputArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            firstOutputArray, secondOutputArray

    let zip clContext = zipWith <@ fun a b -> struct(a, b) @> clContext

    let unzip clContext = unzipWith <@ fun a -> a @> clContext

    let pack clContext = zipWith <@ fun a b -> ((uint64 a) <<< 32) ||| (uint64 b) @> clContext

    let unpack
        (clContext: ClContext)
        workGroupSize =

        let unzipWith = unzipWith <@ fun n -> struct(int (n >>> 32), int n) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<uint64>) ->
            unzipWith processor inputArray

    let map
        (f: Expr<'a -> 'b>)
        (clContext: ClContext)
        workGroupSize =

        let map =
            <@
                fun (range: Range1D)
                    (inputBuffer: ClArray<'a>)
                    (outputBuffer: ClArray<'b>)
                    (length: int) ->

                    let i = range.GlobalID0
                    if i < length then
                        let a = inputBuffer.[i]
                        outputBuffer.[i] <- (%f) a
            @>
        let program = clContext.Compile(map)

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<'a>) ->

            let length = inputArray.Length

            let outputArray = clContext.CreateClArray(length)

            let ndRange = Range1D.CreateValid(length, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray outputArray length)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    let setHeadFlags
        (clContext: ClContext)
        workGroupSize =

        let create = create 0 clContext workGroupSize

        let init =
            <@
                fun (ndRange: Range1D)
                    (inputArray: ClArray<'a>)
                    (heads: ClArray<int>)
                    (size: int) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        let value = inputArray.[i]
                        if i = 0 || inputArray.[i - 1] <> value then heads.[i] <- 1
            @>
        let program = clContext.Compile(init)

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<'a>)
            (outputArray: ClArray<int>) ->

            let size = inputArray.Length

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(size, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            inputArray
                            outputArray
                            size)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let copy (clContext: ClContext) workGroupSize =
        let copy =

            <@ fun (ndRange: Range1D) (inputArrayBuffer: ClArray<'a>) (outputArrayBuffer: ClArray<'a>) inputArrayLength ->

                let i = ndRange.GlobalID0

                if i < inputArrayLength then
                    outputArrayBuffer.[i] <- inputArrayBuffer.[i] @>

        let program = clContext.Compile(copy)

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->
            let ndRange =
                Range1D.CreateValid(inputArray.Length, workGroupSize)

            let outputArray =
                clContext.CreateClArray(inputArray.Length, allocationMode = AllocationMode.Default)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray outputArray inputArray.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    let replicate (clContext: ClContext) =

        let replicate =
            <@ fun (ndRange: Range1D) (inputArrayBuffer: ClArray<'a>) (outputArrayBuffer: ClArray<'a>) inputArrayLength outputArrayLength ->

                let i = ndRange.GlobalID0

                if i < outputArrayLength then
                    outputArrayBuffer.[i] <- inputArrayBuffer.[i % inputArrayLength] @>

        let program = clContext.Compile(replicate)

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) count ->
            let outputArrayLength = inputArray.Length * count

            let outputArray =
                clContext.CreateClArray(outputArrayLength, allocationMode = AllocationMode.Default)

            let ndRange =
                Range1D.CreateValid(outputArray.Length, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange inputArray outputArray inputArray.Length outputArrayLength)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    let private update (clContext: ClContext) =

        let update =
            <@ fun (ndRange: Range1D) inputArrayLength bunchLength (resultBuffer: ClArray<int>) (verticesBuffer: ClArray<int>) ->

                let i = ndRange.GlobalID0 + bunchLength

                if i < inputArrayLength then
                    resultBuffer.[i] <-
                        resultBuffer.[i]
                        + verticesBuffer.[i / bunchLength] @>

        let program = clContext.Compile(update)

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<int>) (inputArrayLength: int) (vertices: ClArray<int>) (bunchLength: int) ->
            let ndRange =
                Range1D.CreateValid(inputArrayLength - bunchLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange inputArrayLength bunchLength inputArray vertices)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private scan (clContext: ClContext) workGroupSize =

        let scan =
            <@ fun (ndRange: Range1D) inputArrayLength verticesLength (resultBuffer: ClArray<int>) (verticesBuffer: ClArray<int>) (totalSumBuffer: ClArray<int>) ->

                let resultLocalBuffer = localArray<int> workGroupSize
                let i = ndRange.GlobalID0
                let localID = ndRange.LocalID0

                if i < inputArrayLength then
                    resultLocalBuffer.[localID] <- resultBuffer.[i]
                else
                    resultLocalBuffer.[localID] <- 0

                let mutable step = 2

                while step <= workGroupSize do
                    barrierLocal ()

                    if localID < workGroupSize / step then
                        let i = step * (localID + 1) - 1

                        resultLocalBuffer.[i] <-
                            resultLocalBuffer.[i]
                            + resultLocalBuffer.[i - (step >>> 1)]

                    step <- step <<< 1

                barrierLocal ()

                if localID = workGroupSize - 1 then
                    if verticesLength <= 1 && localID = i then
                        totalSumBuffer.[0] <- resultLocalBuffer.[localID]

                    verticesBuffer.[i / workGroupSize] <- resultLocalBuffer.[localID]
                    resultLocalBuffer.[localID] <- 0

                step <- workGroupSize

                while step > 1 do
                    barrierLocal ()

                    if localID < workGroupSize / step then
                        let i = step * (localID + 1) - 1
                        let j = i - (step >>> 1)

                        let tmp = resultLocalBuffer.[i]
                        resultLocalBuffer.[i] <- resultLocalBuffer.[i] + resultLocalBuffer.[j]
                        resultLocalBuffer.[j] <- tmp

                    step <- step >>> 1

                barrierLocal ()

                if i < inputArrayLength then
                    resultBuffer.[i] <- resultLocalBuffer.[localID] @>

        let program = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) (inputArrayLength: int) (vertices: ClArray<int>) (verticesLength: int) (totalSum: ClArray<int>) ->
            let ndRange =
                Range1D.CreateValid(inputArrayLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange inputArrayLength verticesLength inputArray vertices totalSum)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    /// <summary>
    /// Exclude inplace prefix sum.
    /// </summary>
    /// <example>
    /// <code>
    /// let arr = [| 1; 2; 3 |]
    /// let sum = [| 0 |]
    /// opencl { do! runExcludeInplace arr sum }
    /// ...
    /// > val arr = [| 0; 1; 3 |]
    /// > val sum = [| 6 |]
    /// </code>
    /// </example>
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let prefixSumExcludeInplace (clContext: ClContext) workGroupSize =

        let scan = scan clContext workGroupSize
        let update = update clContext

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) (totalSum: ClArray<int>) ->
            let firstVertices =
                clContext.CreateClArray<int>(
                    (inputArray.Length - 1) / workGroupSize + 1,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let secondVertices =
                clContext.CreateClArray<int>(
                    (firstVertices.Length - 1) / workGroupSize + 1,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let mutable verticesArrays = firstVertices, secondVertices
            let swap (a, b) = (b, a)
            let mutable verticesLength = firstVertices.Length
            let mutable bunchLength = workGroupSize

            scan processor inputArray inputArray.Length (fst verticesArrays) verticesLength totalSum

            while verticesLength > 1 do
                let fstVertices = fst verticesArrays
                let sndVertices = snd verticesArrays

                scan
                    processor
                    fstVertices
                    verticesLength
                    sndVertices
                    ((verticesLength - 1) / workGroupSize + 1)
                    totalSum

                update processor workGroupSize inputArray inputArray.Length fstVertices bunchLength
                bunchLength <- bunchLength * workGroupSize
                verticesArrays <- swap verticesArrays
                verticesLength <- (verticesLength - 1) / workGroupSize + 1

            processor.Post(Msg.CreateFreeMsg(firstVertices))
            processor.Post(Msg.CreateFreeMsg(secondVertices))

            inputArray, totalSum

    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let prefixSumExclude (clContext: ClContext) workGroupSize =
        let copy = copy clContext workGroupSize

        let prefixSumExcludeInplace =
            prefixSumExcludeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) ->
            let copiedArray = copy processor inputArray

            let totalSum = clContext.CreateClArray [| 0 |]
            prefixSumExcludeInplace processor copiedArray totalSum

    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let prefixSumInclude (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (range: Range1D) (inputArray: ClArray<int>) inputArrayLength (totalSum: ClArray<int>) (outputArray: ClArray<int>) ->

                let gid = range.GlobalID0

                if gid = inputArrayLength - 1 then
                    outputArray.[gid] <- totalSum.[0]
                elif gid < inputArrayLength - 1 then
                    outputArray.[gid] <- inputArray.[gid + 1] @>

        let program = clContext.Compile(kernel)
        let copy = copy clContext workGroupSize

        let prefixSumExcludeInplace =
            prefixSumExcludeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->
            let copiedArray = copy processor inputArray
            let inputArrayLength = inputArray.Length
            let totalSum = clContext.CreateClArray [| 0 |]

            let _, totalSum =
                prefixSumExcludeInplace processor copiedArray totalSum

            let outputArray =
                clContext.CreateClArray(inputArrayLength, allocationMode = AllocationMode.Default)

            let ndRange =
                Range1D.CreateValid(inputArrayLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange copiedArray inputArrayLength totalSum outputArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            processor.Post(Msg.CreateFreeMsg(copiedArray))

            outputArray, totalSum


    let getUniqueBitmap (clContext: ClContext) =

        let getUniqueBitmap =
            <@ fun (ndRange: Range1D) (inputArray: ClArray<'a>) inputLength (isUniqueBitmap: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < inputLength - 1
                   && inputArray.[i] = inputArray.[i + 1] then
                    isUniqueBitmap.[i] <- 0
                else
                    isUniqueBitmap.[i] <- 1 @>

        let getUniqueBitmap = clContext.Compile(getUniqueBitmap)

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) ->

            let inputLength = inputArray.Length

            let ndRange =
                Range1D.CreateValid(inputLength, workGroupSize)

            let bitmap =
                clContext.CreateClArray(
                    inputLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let getUniqueBitmap = getUniqueBitmap.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> getUniqueBitmap.KernelFunc ndRange inputArray inputLength bitmap)
            )

            processor.Post(Msg.CreateRunMsg<_, _> getUniqueBitmap)

            bitmap


    let setPositions (clContext: ClContext) =

        let setPositions =
            <@ fun (ndRange: Range1D) (inputArray: ClArray<'a>) inputLength (positions: ClArray<int>) (outputArray: ClArray<'a>) ->

                let i = ndRange.GlobalID0

                if i < inputLength then
                    outputArray.[positions.[i]] <- inputArray.[i] @>

        let program = clContext.Compile(setPositions)

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) (positions: ClArray<int>) (outputArraySize: int) ->

            let ndRange =
                Range1D.CreateValid(inputArray.Length, workGroupSize)

            let outputArray =
                clContext.CreateClArray(outputArraySize, allocationMode = AllocationMode.Default)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange inputArray inputArray.Length positions outputArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    ///<description>Remove duplicates form the given array.</description>
    ///<param name="clContext">Computational context</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    ///<param name="inputArray">Should be sorted.</param>
    let removeDuplications (clContext: ClContext) workGroupSize =

        let setPositions = setPositions clContext
        let getUniqueBitmap = getUniqueBitmap clContext
        let prefixSumExclude = prefixSumExclude clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let bitmap =
                getUniqueBitmap processor workGroupSize inputArray

            let (positions, sum) = prefixSumExclude processor bitmap

            let resultLength =
                let a = [| 0 |]

                let _ =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(sum, a, ch))

                a.[0]

            let outputArray =
                setPositions processor workGroupSize inputArray positions resultLength

            outputArray
