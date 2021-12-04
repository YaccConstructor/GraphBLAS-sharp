namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations

module internal rec PrefixSum =
    let private update (clContext: ClContext) =
        fun (processor: MailboxProcessor<_>)
            workGroupSize
            (inputArray: ClArray<'a>)
            (inputArrayLength: int)
            (vertices: ClArray<'a>)
            (bunchLength: int)
            (opAdd: Expr<'a -> 'a -> 'a>) ->

            let update =
                <@
                    fun (ndRange: Range1D)
                        (inputArrayLength: int)
                        (bunchLength: int)
                        (resultBuffer: ClArray<'a>)
                        (verticesBuffer: ClArray<'a>) ->

                        let i = ndRange.GlobalID0 + bunchLength

                        if i < inputArrayLength then
                            resultBuffer.[i] <- (%opAdd) verticesBuffer.[i / bunchLength] resultBuffer.[i]
                @>

            let kernel = clContext.CreateClKernel update

            let ndRange = Range1D.CreateValid(inputArrayLength - bunchLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.ArgumentsSetter ndRange inputArrayLength bunchLength inputArray vertices)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private scanExclusive clContext =
        scanGeneral
            <@ fun
                (_: ClArray<'a>)
                (_: 'a)
                (_: int)
                (_: int) ->

                let mutable a = 1
                a <- 1
            @>
            <@
                fun (resultBuffer: ClArray<'a>)
                    (resultLocalBuffer: 'a[])
                    (inputArrayLength: int)
                    (_: int)
                    (i: int)
                    (localID: int) ->

                    if i < inputArrayLength then
                        resultBuffer.[i] <- resultLocalBuffer.[localID]
            @>
            clContext

    let private scanInclusive clContext =
        scanGeneral
            <@
                fun (resultBuffer: ClArray<'a>)
                    (value: 'a)
                    (inputArrayLength: int)
                    (i: int) ->

                    if i < inputArrayLength then
                        resultBuffer.[i] <- value
            @>
            <@
                fun (resultBuffer: ClArray<'a>)
                    (resultLocalBuffer: 'a[])
                    (inputArrayLength: int)
                    (workGroupSize: int)
                    (i: int)
                    (localID: int) ->

                    if i < inputArrayLength && localID < workGroupSize - 1 then
                        resultBuffer.[i] <- resultLocalBuffer.[localID + 1]
            @>
            clContext

    let private scanGeneral
        beforeLocalSumClear
        writeData
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (inputArray: ClArray<'a>)
        (inputArrayLength: int)
        (vertices: ClArray<'a>)
        (verticesLength: int)
        (totalSum: ClArray<'a>)
        idxToWrite
        (opAdd: Expr<'a -> 'a -> 'a>)
        (zero: 'a) =

        let scan =
            <@ fun
                (ndRange: Range1D)
                inputArrayLength
                verticesLength
                (resultBuffer: ClArray<'a>)
                (verticesBuffer: ClArray<'a>)
                (totalSumBuffer: ClArray<'a>) ->

                let resultLocalBuffer = localArray<'a> workGroupSize
                let i = ndRange.GlobalID0
                let localID = ndRange.LocalID0

                if i < inputArrayLength then
                    resultLocalBuffer.[localID] <- resultBuffer.[i]
                else
                    resultLocalBuffer.[localID] <- zero

                let mutable step = 2

                while step <= workGroupSize do
                    barrier ()

                    if localID < workGroupSize / step then
                        let i = step * (localID + 1) - 1

                        resultLocalBuffer.[i] <- (%opAdd) resultLocalBuffer.[i - (step >>> 1)] resultLocalBuffer.[i]

                    step <- step <<< 1

                barrier ()

                if localID = workGroupSize - 1 then
                    if verticesLength <= 1 && localID = i then
                        totalSumBuffer.[idxToWrite] <- resultLocalBuffer.[localID]

                    verticesBuffer.[i / workGroupSize] <- resultLocalBuffer.[localID]
                    (%beforeLocalSumClear) resultBuffer resultLocalBuffer.[localID] inputArrayLength i
                    resultLocalBuffer.[localID] <- zero

                step <- workGroupSize

                while step > 1 do
                    barrier ()

                    if localID < workGroupSize / step then
                        let i = step * (localID + 1) - 1
                        let j = i - (step >>> 1)

                        let tmp = resultLocalBuffer.[i]
                        resultLocalBuffer.[i] <- (%opAdd) resultLocalBuffer.[i] resultLocalBuffer.[j]
                        resultLocalBuffer.[j] <- tmp

                    step <- step >>> 1

                barrier ()

                (%writeData) resultBuffer resultLocalBuffer inputArrayLength workGroupSize i localID
            @>

        let kernel = clContext.CreateClKernel scan
        let ndRange = Range1D.CreateValid(inputArrayLength, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () -> kernel.ArgumentsSetter
                            ndRange
                            inputArrayLength
                            verticesLength
                            inputArray
                            vertices
                            totalSum)
        )
        processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private runInPlace
        scan
        (clContext: ClContext)
        workGroupSize =

        let update = update clContext

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<'a>)
            (totalSum: ClArray<'a>)
            (idxToWrite: int)
            (opAdd: Expr<'a -> 'a -> 'a>)
            (zero: 'a) ->

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

            scan
                clContext
                workGroupSize
                processor
                inputArray
                inputArray.Length
                (fst verticesArrays)
                verticesLength
                totalSum
                idxToWrite
                opAdd
                zero

            while verticesLength > 1 do
                let fstVertices = fst verticesArrays
                let sndVertices = snd verticesArrays

                scanExclusive
                    clContext
                    workGroupSize
                    processor
                    fstVertices
                    verticesLength
                    sndVertices
                    ((verticesLength - 1) / workGroupSize + 1)
                    totalSum
                    idxToWrite
                    opAdd
                    zero

                update processor workGroupSize inputArray inputArray.Length fstVertices bunchLength opAdd
                bunchLength <- bunchLength * workGroupSize
                verticesArrays <- swap verticesArrays
                verticesLength <- (verticesLength - 1) / workGroupSize + 1

            processor.Post(Msg.CreateFreeMsg(firstVertices))
            processor.Post(Msg.CreateFreeMsg(secondVertices))

            inputArray, totalSum

    let runExcludeInplace clContext = runInPlace scanExclusive clContext

    // TODO: нужен ли здесь totalSum?
    // TODO: нужен ли здесь zero?
    let runIncludeInplace clContext = runInPlace scanInclusive clContext
