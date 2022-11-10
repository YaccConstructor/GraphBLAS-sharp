namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations

module internal PrefixSum =
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
    let runExcludeInplace plus = runInplace false scanExclusive plus

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
    let runIncludeInplace plus = runInplace false scanInclusive plus

    let runExclude plus (clContext: ClContext) workGroupSize =

        let runExcludeInplace =
            runExcludeInplace plus clContext workGroupSize

        let copy =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (totalSum: ClCell<'a>) (zero: 'a) ->

            let outputArray = copy processor inputArray

            runExcludeInplace processor outputArray totalSum zero

    let runInclude plus (clContext: ClContext) workGroupSize =

        let runIncludeInplace =
            runIncludeInplace plus clContext workGroupSize

        let copy =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (totalSum: ClCell<'a>) (zero: 'a) ->

            let outputArray = copy processor inputArray

            runIncludeInplace processor outputArray totalSum zero

    let runBackwardsExcludeInplace plus = runInplace true scanExclusive plus

    let runBackwardsIncludeInplace plus = runInplace true scanInclusive plus
