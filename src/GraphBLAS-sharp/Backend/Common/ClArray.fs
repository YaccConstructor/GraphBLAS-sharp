namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common

module ClArray =

    let copy (clContext: ClContext) =
        let copy =
            <@ fun (ndRange: Range1D) (inputArrayBuffer: ClArray<'a>) (outputArrayBuffer: ClArray<'a>) inputArrayLength ->

                let i = ndRange.GlobalID0

                if i < inputArrayLength then
                    outputArrayBuffer.[i] <- inputArrayBuffer.[i] @>

        let kernel = clContext.CreateClKernel copy

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) ->
            let ndRange =
                Range1D(Utils.getDefaultGlobalSize inputArray.Length, workGroupSize)

            let outputArray =
                clContext.CreateClArray(inputArray.Length)

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.SetArguments ndRange inputArray outputArray inputArray.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    let replicate (clContext: ClContext) =
        let replicate =
            <@ fun (ndRange: Range1D) (inputArrayBuffer: ClArray<'a>) (outputArrayBuffer: ClArray<'a>) inputArrayLength outputArrayLength ->

                let i = ndRange.GlobalID0

                if i < outputArrayLength then
                    outputArrayBuffer.[i] <- inputArrayBuffer.[i % inputArrayLength] @>

        let kernel = clContext.CreateClKernel replicate

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) count ->
            let outputArrayLength = inputArray.Length * count

            let outputArray =
                clContext.CreateClArray(outputArrayLength)

            let ndRange =
                Range1D(Utils.getDefaultGlobalSize outputArray.Length, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.SetArguments ndRange inputArray outputArray inputArray.Length outputArrayLength)
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

        let kernel = clContext.CreateClKernel(update)

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<int>) (inputArrayLength: int) (vertices: ClArray<int>) (bunchLength: int) ->
            let ndRange =
                Range1D(
                    Utils.getDefaultGlobalSize inputArrayLength
                    - bunchLength,
                    workGroupSize
                )

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.SetArguments ndRange inputArrayLength bunchLength inputArray vertices)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private scan (clContext: ClContext) =

        let scan =
            <@ fun (ndRange: Range1D) workGroupSize inputArrayLength verticesLength (resultBuffer: ClArray<int>) (verticesBuffer: ClArray<int>) (totalSumBuffer: ClArray<int>) ->

                let resultLocalBuffer = localArray<int> workGroupSize
                let i = ndRange.GlobalID0
                let localID = ndRange.LocalID0

                if i < inputArrayLength then
                    resultLocalBuffer.[localID] <- resultBuffer.[i]
                else
                    resultLocalBuffer.[localID] <- 0

                let mutable step = 2

                while step <= workGroupSize do
                    barrier ()

                    if localID < workGroupSize / step then
                        let i = step * (localID + 1) - 1

                        resultLocalBuffer.[i] <-
                            resultLocalBuffer.[i]
                            + resultLocalBuffer.[i - (step >>> 1)]

                    step <- step <<< 1

                barrier ()

                if localID = workGroupSize - 1 then
                    if verticesLength <= 1 && localID = i then
                        totalSumBuffer.[0] <- resultLocalBuffer.[localID]

                    verticesBuffer.[i / workGroupSize] <- resultLocalBuffer.[localID]
                    resultLocalBuffer.[localID] <- 0

                step <- workGroupSize

                while step > 1 do
                    barrier ()

                    if localID < workGroupSize / step then
                        let i = step * (localID + 1) - 1
                        let j = i - (step >>> 1)

                        let tmp = resultLocalBuffer.[i]
                        resultLocalBuffer.[i] <- resultLocalBuffer.[i] + resultLocalBuffer.[j]
                        resultLocalBuffer.[j] <- tmp

                    step <- step >>> 1

                barrier ()

                if i < inputArrayLength then
                    resultBuffer.[i] <- resultLocalBuffer.[localID] @>

        let kernel = clContext.CreateClKernel(scan)

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<int>) (inputArrayLength: int) (vertices: ClArray<int>) (verticesLength: int) (totalSum: ClArray<int>) ->
            let ndRange =
                Range1D(Utils.getDefaultGlobalSize inputArrayLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.SetArguments
                            ndRange
                            workGroupSize
                            inputArrayLength
                            verticesLength
                            inputArray
                            vertices
                            totalSum)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    /// <summary>
    /// Exclude inplace prefix sum
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
    let prefixSumExcludeInplace (clContext: ClContext) =

        let scan = scan clContext
        let update = update clContext

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<int>) (totalSum: ClArray<int>) ->
            let firstVertices =
                clContext.CreateClArray<int>(
                    (inputArray.Length - 1) / workGroupSize + 1,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let secondVertices =
                clContext.CreateClArray<int>(
                    (firstVertices.Length - 1) / workGroupSize + 1,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let mutable verticesArrays = firstVertices, secondVertices
            let swap (a, b) = (b, a)

            let mutable verticesLength = firstVertices.Length
            let mutable bunchLength = workGroupSize

            scan processor workGroupSize inputArray inputArray.Length (fst verticesArrays) verticesLength totalSum

            while verticesLength > 1 do
                let fstVertices = fst verticesArrays
                let sndVertices = snd verticesArrays

                scan
                    processor
                    workGroupSize
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


    let prefixSumExclude (clContext: ClContext) =
        let copy = copy clContext
        let prefixSumExcludeInplace = prefixSumExcludeInplace clContext

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<int>) ->
            let copiedArray = copy processor workGroupSize inputArray

            let totalSum = clContext.CreateClArray [| 0 |]

            prefixSumExcludeInplace processor workGroupSize copiedArray totalSum


    let prefixSumInclude (clContext: ClContext) =
        let kernel =
            <@ fun (range: Range1D) (inputArray: ClArray<int>) inputArrayLength (totalSum: ClArray<int>) (outputArray: ClArray<int>) ->

                let gid = range.GlobalID0

                if gid = inputArrayLength - 1 then
                    outputArray.[gid] <- totalSum.[0]
                elif gid < inputArrayLength - 1 then
                    outputArray.[gid] <- inputArray.[gid + 1] @>

        let kernel = clContext.CreateClKernel kernel
        let copy = copy clContext
        let prefixSumExcludeInplace = prefixSumExcludeInplace clContext

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) ->
            let copiedArray = copy processor workGroupSize inputArray
            let inputArrayLength = inputArray.Length
            let totalSum = clContext.CreateClArray [| 0 |]

            let _, totalSum =
                prefixSumExcludeInplace processor workGroupSize copiedArray totalSum

            let outputArray = clContext.CreateClArray inputArrayLength

            let ndRange =
                Range1D(Utils.getDefaultGlobalSize inputArrayLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.SetArguments ndRange copiedArray inputArrayLength totalSum outputArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray, totalSum


    let removeDuplications (clContext: ClContext) =
        let getUniqueBitmap =
            <@ fun (ndRange: Range1D) (inputArray: ClArray<'a>) inputLength (isUniqueBitmap: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < inputLength - 1
                   && inputArray.[i] = inputArray.[i + 1] then
                    isUniqueBitmap.[i] <- 0
                else
                    isUniqueBitmap.[i] <- 1 @>

        let setPositions =
            <@ fun (ndRange: Range1D) (inputArray: ClArray<'a>) inputLength (positions: ClArray<int>) (outputArray: ClArray<'a>) ->

                let i = ndRange.GlobalID0

                if i < inputLength then
                    outputArray.[positions.[i]] <- inputArray.[i] @>

        let getUniqueBitmap = clContext.CreateClKernel getUniqueBitmap
        let setPositions = clContext.CreateClKernel setPositions
        let prefixSumExclude = prefixSumExclude clContext

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) ->

            let inputLength = inputArray.Length

            let ndRange =
                Range1D(Utils.getDefaultGlobalSize inputLength, workGroupSize)

            let bitmap = clContext.CreateClArray inputLength

            processor.Post(
                Msg.MsgSetArguments(fun () -> getUniqueBitmap.SetArguments ndRange inputArray inputLength bitmap)
            )

            processor.Post(Msg.CreateRunMsg<_, _> getUniqueBitmap)

            let (positions, sum) =
                prefixSumExclude processor workGroupSize bitmap

            let resultLength =
                let a = [| 0 |]

                let _ =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(sum, a, ch))

                a.[0]

            let outputArray = clContext.CreateClArray resultLength

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> setPositions.SetArguments ndRange inputArray inputLength positions outputArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _> setPositions)

            outputArray
