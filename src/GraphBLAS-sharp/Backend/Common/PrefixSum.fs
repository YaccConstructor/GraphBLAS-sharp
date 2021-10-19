namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL

module internal PrefixSum =

    let private getUpdate (clContext: ClContext) workGroupSize =

        let update =
            <@ fun (ndRange: Range1D) inputArrayLength bunchLength (resultBuffer: ClArray<int>) (verticesBuffer: ClArray<int>) ->

                let i = ndRange.GlobalID0 + bunchLength

                if i < inputArrayLength then
                    resultBuffer.[i] <-
                        resultBuffer.[i]
                        + verticesBuffer.[i / bunchLength] @>

        let kernel = clContext.CreateClKernel(update)

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) (inputArrayLength: int) (vertices: ClArray<int>) (bunchLength: int) ->
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

    let private getScan (clContext: ClContext) workGroupSize =

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

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) (inputArrayLength: int) (vertices: ClArray<int>) (verticesLength: int) (totalSum: ClArray<int>) ->
            let ndRange =
                Range1D(Utils.getDefaultGlobalSize inputArrayLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.SetArguments ndRange inputArrayLength verticesLength inputArray vertices totalSum)
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
    let runExcludeInplace (clContext: ClContext) workGroupSize =

        let scan = getScan clContext workGroupSize
        let update = getUpdate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) (totalSum: ClArray<int>) ->
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

                update processor inputArray inputArray.Length fstVertices bunchLength
                bunchLength <- bunchLength * workGroupSize
                verticesArrays <- swap verticesArrays
                verticesLength <- (verticesLength - 1) / workGroupSize + 1

            processor.Post(Msg.CreateFreeMsg(firstVertices))
            processor.Post(Msg.CreateFreeMsg(secondVertices))

            inputArray, totalSum


    let runExclude (inputArray: int []) =
        opencl {
            let! copiedArray = Copy.copyArray inputArray

            let totalSum = [| 0 |]
            failwith "FIX ME! And rewrite."
            //do! runExcludeInplace copiedArray totalSum

            return (copiedArray, totalSum)
        }

    let runInclude (inputArray: int []) =
        opencl {
            if inputArray.Length = 0 then
                return [||], [| 0 |]
            else
                let! copiedArray = Copy.copyArray inputArray

                let totalSum = [| 0 |]
                failwith "FIX ME! And rewrite."
                //do! runExcludeInplace copiedArray totalSum

                let wgSize = Utils.defaultWorkGroupSize
                let length = inputArray.Length

                let kernel =
                    <@ fun (range: Range1D) (array: int []) (totalSum: int []) (outputArray: int []) ->

                        let gid = range.GlobalID0

                        if gid = length - 1 then
                            outputArray.[gid] <- totalSum.[0]
                        elif gid < length - 1 then
                            outputArray.[gid] <- array.[gid + 1] @>

                let outputArray = Array.zeroCreate length

                do!
                    runCommand kernel
                    <| fun kernelPrepare ->
                        kernelPrepare
                        <| Range1D(Utils.getDefaultGlobalSize length, wgSize)
                        <| copiedArray
                        <| totalSum
                        <| outputArray

                return outputArray, totalSum
        }
