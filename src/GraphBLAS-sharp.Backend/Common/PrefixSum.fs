namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations

module internal PrefixSum =
    let private update
        (opAdd: Expr<'a -> 'a -> 'a>)
        (clContext: ClContext)
        workGroupSize =

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
        let kernel = clContext.CreateClProgram(update).GetKernel()

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<'a>)
            (inputArrayLength: int)
            (vertices: ClArray<'a>)
            (bunchLength: int) ->

            let ndRange = Range1D.CreateValid(inputArrayLength - bunchLength, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange inputArrayLength bunchLength inputArray vertices)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private scanGeneral
        beforeLocalSumClear
        writeData
        (opAdd: Expr<'a -> 'a -> 'a>)
        (clContext: ClContext)
        workGroupSize =

        let scan =
            <@ fun
                (ndRange: Range1D)
                inputArrayLength
                verticesLength
                (resultBuffer: ClArray<'a>)
                (verticesBuffer: ClArray<'a>)
                (totalSumBuffer: ClCell<'a>)
                (zero: ClCell<'a>) ->

                let resultLocalBuffer = localArray<'a> workGroupSize
                let i = ndRange.GlobalID0
                let localID = ndRange.LocalID0

                let zero = zero.Value

                if i < inputArrayLength then
                    resultLocalBuffer.[localID] <- resultBuffer.[i]
                else
                    resultLocalBuffer.[localID] <- zero

                let mutable step = 2

                while step <= workGroupSize do
                    barrierLocal ()

                    if localID < workGroupSize / step then
                        let i = step * (localID + 1) - 1

                        let buff = (%opAdd) resultLocalBuffer.[i - (step >>> 1)] resultLocalBuffer.[i]
                        resultLocalBuffer.[i] <- buff

                    step <- step <<< 1

                barrierLocal ()

                if localID = workGroupSize - 1 then
                    if verticesLength <= 1 && localID = i then
                        totalSumBuffer.Value <- resultLocalBuffer.[localID]

                    verticesBuffer.[i / workGroupSize] <- resultLocalBuffer.[localID]
                    (%beforeLocalSumClear) resultBuffer resultLocalBuffer.[localID] inputArrayLength i
                    resultLocalBuffer.[localID] <- zero

                step <- workGroupSize

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

                (%writeData) resultBuffer resultLocalBuffer inputArrayLength workGroupSize i localID
            @>
        let kernel = clContext.CreateClProgram(scan).GetKernel()

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<'a>)
            (inputArrayLength: int)
            (vertices: ClArray<'a>)
            (verticesLength: int)
            (totalSum: ClCell<'a>)
            (zero: 'a) ->

            // TODO: передавать zero как константу
            let zero = clContext.CreateClCell(zero)

            let ndRange = Range1D.CreateValid(inputArrayLength, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc
                                ndRange
                                inputArrayLength
                                verticesLength
                                inputArray
                                vertices
                                totalSum
                                zero)
            )
            processor.Post(Msg.CreateRunMsg<_, _> kernel)
            processor.Post(Msg.CreateFreeMsg(zero))

    let private scanExclusive<'a when 'a: struct> =
        scanGeneral
            <@
                fun (a: ClArray<'a>)
                    (b: 'a)
                    (c: int)
                    (d: int) ->

                    let mutable x = 1
                    x <- 1
            @>
            <@
                fun (resultBuffer: ClArray<'a>)
                    (resultLocalBuffer: 'a[])
                    (inputArrayLength: int)
                    (smth: int)
                    (i: int)
                    (localID: int) ->

                    if i < inputArrayLength then
                        resultBuffer.[i] <- resultLocalBuffer.[localID]
            @>

    let private scanInclusive<'a when 'a: struct> =
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

    let private runInPlace
        scan
        (opAdd: Expr<'a -> 'a -> 'a>)
        (clContext: ClContext)
        workGroupSize =

        let scan = scan opAdd clContext workGroupSize
        let scanExclusive = scanExclusive opAdd clContext workGroupSize
        let update = update opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<'a>)
            (totalSum: ClCell<'a>)
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
                processor
                inputArray
                inputArray.Length
                (fst verticesArrays)
                verticesLength
                totalSum
                zero

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

                update processor inputArray inputArray.Length fstVertices bunchLength
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
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    ///<param name="processor">.</param>
    ///<param name="inputArray">.</param>
    ///<param name="totalSum">.</param>
    ///<param name="plus">Associative binary operation.</param>
    ///<param name="zero">Zero element for binary operation.</param>
    let runIncludeInplace plus = runInPlace scanInclusive plus

    let runExclude
        plus
        (clContext: ClContext)
        workGroupSize =

        let runExcludeInplace = runExcludeInplace plus clContext workGroupSize
        let copy = GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<'a>)
            (totalSum: ClCell<'a>)
            (zero: 'a) ->

            let outputArray = copy processor inputArray

            runExcludeInplace
                processor
                outputArray
                totalSum
                zero

    let runInclude
        plus
        (clContext: ClContext)
        workGroupSize =

        let runIncludeInplace = runIncludeInplace plus clContext workGroupSize
        let copy = GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<'a>)
            (totalSum: ClCell<'a>)
            (zero: 'a) ->

            let outputArray = copy processor inputArray

            runIncludeInplace
                processor
                outputArray
                totalSum
                zero
