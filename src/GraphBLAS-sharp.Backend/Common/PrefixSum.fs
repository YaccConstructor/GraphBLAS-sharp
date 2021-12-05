namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations

module internal rec PrefixSum =
    let private update
        (clContext: ClContext)
        (processor: MailboxProcessor<_>)
        workGroupSize
        (inputArray: ClArray<'a>)
        (inputArrayLength: int)
        (vertices: ClArray<'a>)
        (bunchLength: int)
        (opAdd: Expr<'a -> 'a -> 'a>) =

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
        (opAdd: Expr<'a -> 'a -> 'a>)
        (zero: 'a) =

        // TODO: сделать красивее
        let zero = clContext.CreateClArray([|zero|])

        let scan =
            <@ fun
                (ndRange: Range1D)
                inputArrayLength
                verticesLength
                (resultBuffer: ClArray<'a>)
                (verticesBuffer: ClArray<'a>)
                (totalSumBuffer: ClArray<'a>)
                (zero: ClArray<'a>) ->

                let resultLocalBuffer = localArray<'a> workGroupSize
                let i = ndRange.GlobalID0
                let localID = ndRange.LocalID0

                let zero = zero.[0]

                if i < inputArrayLength then
                    resultLocalBuffer.[localID] <- resultBuffer.[i]
                else
                    resultLocalBuffer.[localID] <- zero

                let mutable step = 2

                while step <= workGroupSize do
                    barrier ()

                    if localID < workGroupSize / step then
                        let i = step * (localID + 1) - 1

                        let buff = (%opAdd) resultLocalBuffer.[i - (step >>> 1)] resultLocalBuffer.[i]
                        resultLocalBuffer.[i] <- buff

                    step <- step <<< 1

                barrier ()

                if localID = workGroupSize - 1 then
                    if verticesLength <= 1 && localID = i then
                        totalSumBuffer.[0] <- resultLocalBuffer.[localID]

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
                        let buff = (%opAdd) tmp resultLocalBuffer.[j]
                        resultLocalBuffer.[i] <- buff
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
                            totalSum
                            zero)
        )
        processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private runInPlace
        scan
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (inputArray: ClArray<'a>)
        (totalSum: ClArray<'a>)
        (opAdd: Expr<'a -> 'a -> 'a>)
        (zero: 'a) =

        let update = update clContext

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
                opAdd
                zero

            update processor workGroupSize inputArray inputArray.Length fstVertices bunchLength opAdd
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
    let runExcludeInplace
        clContext
        workGroupSize
        processor
        inputArray
        totalSum
        plus
        zero =
        runInPlace
            scanExclusive
            clContext
            workGroupSize
            processor
            inputArray
            totalSum
            plus
            zero

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
    let runIncludeInplace clContext = runInPlace scanInclusive clContext
