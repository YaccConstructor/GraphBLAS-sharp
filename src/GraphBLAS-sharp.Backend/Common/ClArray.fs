namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common
open Brahma.FSharp
open Microsoft.FSharp.Quotations

module ClArray =
    let init (initializer: Expr<int -> 'a>) (clContext: ClContext) workGroupSize =

        let init =
            <@ fun (range: Range1D) (outputBuffer: ClArray<'a>) (length: int) ->

                let i = range.GlobalID0

                if i < length then
                    outputBuffer.[i] <- (%initializer) i @>

        let program = clContext.Compile(init)

        fun (processor: MailboxProcessor<_>) (length: int) ->
            // TODO: Выставить нужные флаги
            let outputArray = clContext.CreateClArray(length)

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

        fun (processor: MailboxProcessor<_>) (length: int) (value: 'a) ->
            let value = clContext.CreateClCell(value)

            let outputArray = clContext.CreateClArray(length)

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(length, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange outputArray length value))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)
            processor.Post(Msg.CreateFreeMsg(value))

            outputArray

    let zeroCreate (clContext: ClContext) workGroupSize =

        let create = create clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (length: int) -> create processor length Unchecked.defaultof<'a>

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

        let kernel = clContext.Compile(replicate)

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) count ->
            let outputArrayLength = inputArray.Length * count

            let outputArray =
                clContext.CreateClArray(outputArrayLength, allocationMode = AllocationMode.Default)

            let ndRange =
                Range1D.CreateValid(outputArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange inputArray outputArray inputArray.Length outputArrayLength)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    let private update (opAdd: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =

        let update =
            <@ fun (ndRange: Range1D) (inputArrayLength: int) (bunchLength: int) (resultBuffer: ClArray<'a>) (verticesBuffer: ClArray<'a>) (mirror: ClCell<bool>) ->

                let mirror = mirror.Value

                let mutable i = ndRange.GlobalID0 + bunchLength
                let gid = i

                if mirror then
                    i <- inputArrayLength - 1 - i

                if gid < inputArrayLength then
                    resultBuffer.[i] <- (%opAdd) verticesBuffer.[gid / bunchLength] resultBuffer.[i] @>

        let program = clContext.Compile(update)

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (inputArrayLength: int) (vertices: ClArray<'a>) (bunchLength: int) (mirror: bool) ->

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(inputArrayLength - bunchLength, workGroupSize)

            let mirror = clContext.CreateClCell mirror

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange inputArrayLength bunchLength inputArray vertices mirror)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)
            processor.Post(Msg.CreateFreeMsg(mirror))

    let private scanGeneral
        beforeLocalSumClear
        writeData
        (opAdd: Expr<'a -> 'a -> 'a>)
        (clContext: ClContext)
        workGroupSize
        =

        let subSum = SubSum.treeSum opAdd

        let scan =
            <@ fun (ndRange: Range1D) inputArrayLength verticesLength (resultBuffer: ClArray<'a>) (verticesBuffer: ClArray<'a>) (totalSumBuffer: ClCell<'a>) (zero: ClCell<'a>) (mirror: ClCell<bool>) ->

                let mirror = mirror.Value

                let resultLocalBuffer = localArray<'a> workGroupSize
                let mutable i = ndRange.GlobalID0
                let gid = i

                if mirror then
                    i <- inputArrayLength - 1 - i

                let localID = ndRange.LocalID0

                let zero = zero.Value

                if gid < inputArrayLength then
                    resultLocalBuffer.[localID] <- resultBuffer.[i]
                else
                    resultLocalBuffer.[localID] <- zero

                barrierLocal ()

                (%subSum) workGroupSize localID resultLocalBuffer

                if localID = workGroupSize - 1 then
                    if verticesLength <= 1 && localID = gid then
                        totalSumBuffer.Value <- resultLocalBuffer.[localID]

                    verticesBuffer.[gid / workGroupSize] <- resultLocalBuffer.[localID]
                    (%beforeLocalSumClear) resultBuffer resultLocalBuffer.[localID] inputArrayLength gid i
                    resultLocalBuffer.[localID] <- zero

                let mutable step = workGroupSize

                while step > 1 do
                    barrierLocal ()

                    if localID < workGroupSize / step then
                        let i = step * (localID + 1) - 1
                        let j = i - (step >>> 1)

                        let tmp = resultLocalBuffer.[i]
                        let buff = (%opAdd) tmp resultLocalBuffer.[j]
                        resultLocalBuffer.[i] <- buff
                        resultLocalBuffer.[j] <- tmp

                    step <- step >>> 1

                barrierLocal ()

                (%writeData) resultBuffer resultLocalBuffer inputArrayLength workGroupSize gid i localID @>

        let program = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (inputArrayLength: int) (vertices: ClArray<'a>) (verticesLength: int) (totalSum: ClCell<'a>) (zero: 'a) (mirror: bool) ->

            // TODO: передавать zero как константу
            let zero = clContext.CreateClCell(zero)

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(inputArrayLength, workGroupSize)

            let mirror = clContext.CreateClCell mirror

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            inputArrayLength
                            verticesLength
                            inputArray
                            vertices
                            totalSum
                            zero
                            mirror)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)
            processor.Post(Msg.CreateFreeMsg(zero))
            processor.Post(Msg.CreateFreeMsg(mirror))

    let private scanExclusive<'a when 'a: struct> =
        scanGeneral
            <@ fun (a: ClArray<'a>) (b: 'a) (c: int) (d: int) (e: int) ->

                () @>
            <@ fun (resultBuffer: ClArray<'a>) (resultLocalBuffer: 'a []) (inputArrayLength: int) (smth: int) (gid: int) (i: int) (localID: int) ->

                if gid < inputArrayLength then
                    resultBuffer.[i] <- resultLocalBuffer.[localID] @>

    let private scanInclusive<'a when 'a: struct> =
        scanGeneral
            <@ fun (resultBuffer: ClArray<'a>) (value: 'a) (inputArrayLength: int) (gid: int) (i: int) ->

                if gid < inputArrayLength then
                    resultBuffer.[i] <- value @>
            <@ fun (resultBuffer: ClArray<'a>) (resultLocalBuffer: 'a []) (inputArrayLength: int) (workGroupSize: int) (gid: int) (i: int) (localID: int) ->

                if gid < inputArrayLength
                   && localID < workGroupSize - 1 then
                    resultBuffer.[i] <- resultLocalBuffer.[localID + 1] @>

    let private runInplace (mirror: bool) scan (opAdd: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =

        let scan = scan opAdd clContext workGroupSize

        let scanExclusive =
            scanExclusive opAdd clContext workGroupSize

        let update = update opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (totalSum: ClCell<'a>) (zero: 'a) ->

            let firstVertices =
                clContext.CreateClArray<'a>(
                    (inputArray.Length - 1) / workGroupSize + 1,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let secondVertices =
                clContext.CreateClArray<'a>(
                    (firstVertices.Length - 1) / workGroupSize + 1,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let mutable verticesArrays = firstVertices, secondVertices
            let swap (a, b) = (b, a)
            let mutable verticesLength = firstVertices.Length
            let mutable bunchLength = workGroupSize

            scan processor inputArray inputArray.Length (fst verticesArrays) verticesLength totalSum zero mirror

            while verticesLength > 1 do
                let fstVertices = fst verticesArrays
                let sndVertices = snd verticesArrays

                scanExclusive
                    processor
                    fstVertices
                    verticesLength
                    sndVertices
                    ((verticesLength - 1) / workGroupSize + 1)
                    totalSum
                    zero
                    false

                update processor inputArray inputArray.Length fstVertices bunchLength mirror
                bunchLength <- bunchLength * workGroupSize
                verticesArrays <- swap verticesArrays
                verticesLength <- (verticesLength - 1) / workGroupSize + 1

            processor.Post(Msg.CreateFreeMsg(firstVertices))
            processor.Post(Msg.CreateFreeMsg(secondVertices))

            inputArray, totalSum

    /// <summary>
    /// Exclude inplace prefix sum.
    /// </summary>
    /// <example>
    /// <code>
    /// let arr = [| 1; 1; 1; 1 |]
    /// let sum = [| 0 |]
    /// runExcludeInplace clContext workGroupSize processor arr sum <@ (+) @> 0
    /// |> ignore
    /// ...
    /// > val arr = [| 0; 1; 2; 3 |]
    /// > val sum = [| 4 |]
    /// </code>
    /// </example>
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    ///<param name="processor">.</param>
    ///<param name="inputArray">.</param>
    ///<param name="totalSum">.</param>
    ///<param name="plus">Associative binary operation.</param>
    ///<param name="zero">Zero element for binary operation.</param>
    let prefixSumExcludeInplace plus = runInplace false scanExclusive plus

    /// <summary>
    /// Include inplace prefix sum.
    /// </summary>
    /// <example>
    /// <code>
    /// let arr = [| 1; 1; 1; 1 |]
    /// let sum = [| 0 |]
    /// runExcludeInplace clContext workGroupSize processor arr sum <@ (+) @> 0
    /// |> ignore
    /// ...
    /// > val arr = [| 1; 2; 3; 4 |]
    /// > val sum = [| 4 |]
    /// </code>
    /// </example>
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    ///<param name="processor">.</param>
    ///<param name="inputArray">.</param>
    ///<param name="totalSum">.</param>
    ///<param name="plus">Associative binary operation.</param>
    ///<param name="zero">Zero element for binary operation.</param>
    let prefixSumIncludeInplace plus = runInplace false scanInclusive plus

    let prefixSumExclude plus (clContext: ClContext) workGroupSize =

        let runExcludeInplace =
            prefixSumExcludeInplace plus clContext workGroupSize

        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (totalSum: ClCell<'a>) (zero: 'a) ->

            let outputArray = copy processor inputArray

            runExcludeInplace processor outputArray totalSum zero

    let prefixSumInclude plus (clContext: ClContext) workGroupSize =

        let runIncludeInplace =
            prefixSumIncludeInplace plus clContext workGroupSize

        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (totalSum: ClCell<'a>) (zero: 'a) ->

            let outputArray = copy processor inputArray

            runIncludeInplace processor outputArray totalSum zero

    let prefixSumBackwardsExcludeInplace plus = runInplace true scanExclusive plus

    let prefixSumBackwardsIncludeInplace plus = runInplace true scanInclusive plus

    let getUniqueBitmap (clContext: ClContext) =

        let getUniqueBitmap =
            <@ fun (ndRange: Range1D) (inputArray: ClArray<'a>) inputLength (isUniqueBitmap: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < inputLength - 1
                   && inputArray.[i] = inputArray.[i + 1] then
                    isUniqueBitmap.[i] <- 0
                else
                    isUniqueBitmap.[i] <- 1 @>

        let kernel = clContext.Compile(getUniqueBitmap)

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

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray inputLength bitmap))

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            bitmap

    let setPositions (clContext: ClContext) =

        let setPositions =
            <@ fun (ndRange: Range1D) (inputArray: ClArray<int>) inputLength (positions: ClArray<int>) (outputArray: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < inputLength then
                    outputArray.[positions.[i]] <- inputArray.[i] @>

        let kernel = clContext.Compile(setPositions)

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) (positions: ClArray<int>) (outputArraySize: int) ->

            let ndRange =
                Range1D.CreateValid(inputArray.Length, workGroupSize)

            let outputArray =
                clContext.CreateClArray(outputArraySize, allocationMode = AllocationMode.Default)

            let kernel = kernel.GetKernel()

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

        let prefixSumExclude =
            prefixSumExclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let bitmap =
                getUniqueBitmap processor workGroupSize inputArray

            let sum = clContext.CreateClCell 0

            let positions, sum = prefixSumExclude processor bitmap sum 0

            let resultLength =
                let a = [| 0 |]

                processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(sum, a, ch))
                |> ignore

                processor.Post(Msg.CreateFreeMsg<_>(sum))

                a.[0]

            let outputArray =
                setPositions processor workGroupSize inputArray positions resultLength

            outputArray
