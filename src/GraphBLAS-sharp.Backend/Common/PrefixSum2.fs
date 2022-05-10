namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations

module internal PrefixSum2 =
    let private update
        (opAdd: Expr<'a -> 'b -> 'a -> 'b -> 'a * 'b>)
        (clContext: ClContext)
        workGroupSize =

        let update =
            <@
                fun (ndRange: Range1D)
                    (inputArrayLength: int)
                    (bunchLength: int)
                    (resultBuffer0: ClArray<'a>)
                    (resultBuffer1: ClArray<'b>)
                    (verticesBuffer0: ClArray<'a>)
                    (verticesBuffer1: ClArray<'b>) ->

                    let i = ndRange.GlobalID0 + bunchLength

                    if i < inputArrayLength then
                        let (x, y) =
                            (%opAdd) verticesBuffer0.[i / bunchLength] verticesBuffer1.[i / bunchLength] resultBuffer0.[i] resultBuffer1.[i]
                        resultBuffer0.[i] <- x
                        resultBuffer1.[i] <- y
            @>
        let program = clContext.CreateClProgram(update)

        fun (processor: MailboxProcessor<_>)
            (inputArray0: ClArray<'a>)
            (inputArray1: ClArray<'b>)
            (inputArrayLength: int)
            (vertices0: ClArray<'a>)
            (vertices1: ClArray<'b>)
            (bunchLength: int) ->

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(inputArrayLength - bunchLength, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            inputArrayLength
                            bunchLength
                            inputArray0
                            inputArray1
                            vertices0
                            vertices1)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private scanGeneral
        beforeLocalSumClear
        writeData
        (opAdd: Expr<'a -> 'b -> 'a -> 'b -> 'a * 'b>)
        (clContext: ClContext)
        workGroupSize =

        let scan =
            <@ fun
                (ndRange: Range1D)
                inputArrayLength
                verticesLength
                (resultBuffer0: ClArray<'a>)
                (resultBuffer1: ClArray<'b>)
                (verticesBuffer0: ClArray<'a>)
                (verticesBuffer1: ClArray<'b>)
                (totalSumBuffer0: ClCell<'a>)
                (totalSumBuffer1: ClCell<'b>)
                (zero0: ClCell<'a>)
                (zero1: ClCell<'b>) ->

                let resultLocalBuffer0 = localArray<'a> workGroupSize
                let resultLocalBuffer1 = localArray<'b> workGroupSize
                let i = ndRange.GlobalID0
                let localID = ndRange.LocalID0

                let zero0 = zero0.Value
                let zero1 = zero1.Value

                if i < inputArrayLength then
                    resultLocalBuffer0.[localID] <- resultBuffer0.[i]
                    resultLocalBuffer1.[localID] <- resultBuffer1.[i]
                else
                    resultLocalBuffer0.[localID] <- zero0
                    resultLocalBuffer1.[localID] <- zero1

                let mutable step = 2

                while step <= workGroupSize do
                    barrierLocal ()

                    if localID < workGroupSize / step then
                        let i = step * (localID + 1) - 1

                        let (buff0, buff1) =
                            (%opAdd) resultLocalBuffer0.[i - (step >>> 1)] resultLocalBuffer1.[i - (step >>> 1)]
                                resultLocalBuffer0.[i] resultLocalBuffer1.[i]
                        resultLocalBuffer0.[i] <- buff0
                        resultLocalBuffer1.[i] <- buff1

                    step <- step <<< 1

                barrierLocal ()

                if localID = workGroupSize - 1 then
                    if verticesLength <= 1 && localID = i then
                        totalSumBuffer0.Value <- resultLocalBuffer0.[localID]
                        totalSumBuffer1.Value <- resultLocalBuffer1.[localID]

                    verticesBuffer0.[i / workGroupSize] <- resultLocalBuffer0.[localID]
                    verticesBuffer1.[i / workGroupSize] <- resultLocalBuffer1.[localID]
                    (%beforeLocalSumClear) resultBuffer0 resultBuffer1 resultLocalBuffer0.[localID] resultLocalBuffer1.[localID] inputArrayLength i
                    resultLocalBuffer0.[localID] <- zero0
                    resultLocalBuffer1.[localID] <- zero1

                step <- workGroupSize

                while step > 1 do
                    barrierLocal ()

                    if localID < workGroupSize / step then
                        let i = step * (localID + 1) - 1
                        let j = i - (step >>> 1)

                        let tmp0 = resultLocalBuffer0.[i]
                        let tmp1 = resultLocalBuffer1.[i]
                        let (buff0, buff1) = (%opAdd) tmp0 tmp1 resultLocalBuffer0.[j] resultLocalBuffer1.[j]
                        resultLocalBuffer0.[i] <- buff0
                        resultLocalBuffer1.[i] <- buff1
                        resultLocalBuffer0.[j] <- tmp0
                        resultLocalBuffer1.[j] <- tmp1

                    step <- step >>> 1

                barrierLocal ()

                (%writeData) resultBuffer0 resultBuffer1 resultLocalBuffer0 resultLocalBuffer1 inputArrayLength workGroupSize i localID
            @>
        let program = clContext.CreateClProgram(scan)

        fun (processor: MailboxProcessor<_>)
            (inputArray0: ClArray<'a>)
            (inputArray1: ClArray<'b>)
            (inputArrayLength: int)
            (vertices0: ClArray<'a>)
            (vertices1: ClArray<'b>)
            (verticesLength: int)
            (totalSum0: ClCell<'a>)
            (totalSum1: ClCell<'b>)
            (zero0: 'a)
            (zero1: 'b) ->

            // TODO: передавать zero как константу
            let zero0 = clContext.CreateClCell(zero0)
            let zero1 = clContext.CreateClCell(zero1)

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(inputArrayLength, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc
                                ndRange
                                inputArrayLength
                                verticesLength
                                inputArray0
                                inputArray1
                                vertices0
                                vertices1
                                totalSum0
                                totalSum1
                                zero0
                                zero1)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernel)
            processor.Post(Msg.CreateFreeMsg(zero0))
            processor.Post(Msg.CreateFreeMsg(zero1))

    let private scanExclusive<'a, 'b when 'a: struct and 'b: struct> =
        scanGeneral
            <@
                fun (a0: ClArray<'a>)
                    (a1: ClArray<'b>)
                    (b0: 'a)
                    (b1: 'b)
                    (c: int)
                    (d: int) ->

                    let mutable x = 1
                    x <- 1
            @>
            <@
                fun (resultBuffer0: ClArray<'a>)
                    (resultBuffer1: ClArray<'b>)
                    (resultLocalBuffer0: 'a[])
                    (resultLocalBuffer1: 'b[])
                    (inputArrayLength: int)
                    (smth: int)
                    (i: int)
                    (localID: int) ->

                    if i < inputArrayLength then
                        resultBuffer0.[i] <- resultLocalBuffer0.[localID]
                        resultBuffer1.[i] <- resultLocalBuffer1.[localID]
            @>

    let private scanInclusive<'a, 'b when 'a: struct and 'b: struct> =
        scanGeneral
            <@
                fun (resultBuffer0: ClArray<'a>)
                    (resultBuffer1: ClArray<'b>)
                    (value0: 'a)
                    (value1: 'b)
                    (inputArrayLength: int)
                    (i: int) ->

                    if i < inputArrayLength then
                        resultBuffer0.[i] <- value0
                        resultBuffer1.[i] <- value1
            @>
            <@
                fun (resultBuffer0: ClArray<'a>)
                    (resultBuffer1: ClArray<'b>)
                    (resultLocalBuffer0: 'a[])
                    (resultLocalBuffer1: 'b[])
                    (inputArrayLength: int)
                    (workGroupSize: int)
                    (i: int)
                    (localID: int) ->

                    if i < inputArrayLength && localID < workGroupSize - 1 then
                        resultBuffer0.[i] <- resultLocalBuffer0.[localID + 1]
                        resultBuffer1.[i] <- resultLocalBuffer1.[localID + 1]
            @>

    let private runInPlace
        scan
        (opAdd: Expr<'a -> 'b -> 'a -> 'b -> 'a * 'b>)
        (clContext: ClContext)
        workGroupSize =

        let scan = scan opAdd clContext workGroupSize
        let scanExclusive = scanExclusive opAdd clContext workGroupSize
        let update = update opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray0: ClArray<'a>)
            (inputArray1: ClArray<'b>)
            (totalSum0: ClCell<'a>)
            (totalSum1: ClCell<'b>)
            (zero0: 'a)
            (zero1: 'b) ->

            let firstVertices0 =
                clContext.CreateClArray<'a>(
                    (inputArray0.Length - 1) / workGroupSize + 1,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let secondVertices0 =
                clContext.CreateClArray<'a>(
                    (firstVertices0.Length - 1) / workGroupSize + 1,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let firstVertices1 =
                clContext.CreateClArray<'b>(
                    (inputArray1.Length - 1) / workGroupSize + 1,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let secondVertices1 =
                clContext.CreateClArray<'b>(
                    (firstVertices1.Length - 1) / workGroupSize + 1,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let mutable verticesArrays0 = firstVertices0, secondVertices0
            let mutable verticesArrays1 = firstVertices1, secondVertices1
            let swap (a, b) = (b, a)
            let mutable verticesLength = firstVertices0.Length
            let mutable bunchLength = workGroupSize

            scan
                processor
                inputArray0
                inputArray1
                inputArray0.Length
                (fst verticesArrays0)
                (fst verticesArrays1)
                verticesLength
                totalSum0
                totalSum1
                zero0
                zero1

            while verticesLength > 1 do
                let fstVertices0 = fst verticesArrays0
                let sndVertices0 = snd verticesArrays0
                let fstVertices1 = fst verticesArrays1
                let sndVertices1 = snd verticesArrays1

                scanExclusive
                    processor
                    fstVertices0
                    fstVertices1
                    verticesLength
                    sndVertices0
                    sndVertices1
                    ((verticesLength - 1) / workGroupSize + 1)
                    totalSum0
                    totalSum1
                    zero0
                    zero1

                update processor inputArray0 inputArray1 inputArray0.Length fstVertices0 fstVertices1 bunchLength
                bunchLength <- bunchLength * workGroupSize
                verticesArrays0 <- swap verticesArrays0
                verticesArrays1 <- swap verticesArrays1
                verticesLength <- (verticesLength - 1) / workGroupSize + 1

            processor.Post(Msg.CreateFreeMsg(firstVertices0))
            processor.Post(Msg.CreateFreeMsg(firstVertices1))
            processor.Post(Msg.CreateFreeMsg(secondVertices0))
            processor.Post(Msg.CreateFreeMsg(secondVertices1))

            inputArray0, inputArray1, totalSum0, totalSum1

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
    let runExcludeInplace plus = runInPlace scanExclusive plus

    // TODO: нужен ли totalSum
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
    let runIncludeInplace plus = runInPlace scanInclusive plus

    let runExclude
        plus
        (clContext: ClContext)
        workGroupSize =

        let runExcludeInplace = runExcludeInplace plus clContext workGroupSize
        let copy0 = GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize
        let copy1 = GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray0: ClArray<'a>)
            (inputArray1: ClArray<'b>)
            (totalSum0: ClCell<'a>)
            (totalSum1: ClCell<'b>)
            (zero0: 'a)
            (zero1: 'b) ->

            let outputArray0 = copy0 processor inputArray0
            let outputArray1 = copy1 processor inputArray1

            runExcludeInplace
                processor
                outputArray0
                outputArray1
                totalSum0
                totalSum1
                zero0
                zero1

    let runInclude
        plus
        (clContext: ClContext)
        workGroupSize =

        let runIncludeInplace = runIncludeInplace plus clContext workGroupSize
        let copy0 = GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize
        let copy1 = GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray0: ClArray<'a>)
            (inputArray1: ClArray<'b>)
            (totalSum0: ClCell<'a>)
            (totalSum1: ClCell<'b>)
            (zero0: 'a)
            (zero1: 'b) ->

            let outputArray0 = copy0 processor inputArray0
            let outputArray1 = copy1 processor inputArray1

            runIncludeInplace
                processor
                outputArray0
                outputArray1
                totalSum0
                totalSum1
                zero0
                zero1
