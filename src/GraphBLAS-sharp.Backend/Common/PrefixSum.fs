namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClCellExtensions

module internal PrefixSumInternal =
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
            mirror.Free processor

    let private scanGeneral
        beforeLocalSumClear
        writeData
        (opAdd: Expr<'a -> 'a -> 'a>)
        (clContext: ClContext)
        workGroupSize
        =

        let scan =
            <@ fun (ndRange: Range1D) inputArrayLength verticesLength (inputArray: ClArray<'a>) (verticesBuffer: ClArray<'a>) (totalSumBuffer: ClCell<'a>) (zero: ClCell<'a>) (mirror: ClCell<bool>) ->

                let mirror = mirror.Value

                let resultLocalBuffer = localArray<'a> workGroupSize
                let mutable i = ndRange.GlobalID0
                let gid = i

                if mirror then
                    i <- inputArrayLength - 1 - i

                let lid = ndRange.LocalID0

                let zero = zero.Value

                if gid < inputArrayLength then
                    resultLocalBuffer.[lid] <- inputArray.[i]
                else
                    resultLocalBuffer.[lid] <- zero

                barrierLocal ()

                // Local tree reduce
                (%SubSum.upSweep opAdd) workGroupSize lid resultLocalBuffer

                if lid = workGroupSize - 1 then
                    // if last iteration
                    if verticesLength <= 1 && lid = gid then
                        totalSumBuffer.Value <- resultLocalBuffer.[lid]

                    verticesBuffer.[gid / workGroupSize] <- resultLocalBuffer.[lid]
                    (%beforeLocalSumClear) inputArray resultLocalBuffer.[lid] inputArrayLength gid i
                    resultLocalBuffer.[lid] <- zero

                (%SubSum.downSweep opAdd) workGroupSize lid resultLocalBuffer

                barrierLocal ()

                (%writeData) inputArray resultLocalBuffer inputArrayLength workGroupSize gid i lid @>

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

            zero.Free processor
            mirror.Free processor

    let private scanExclusive<'a when 'a: struct> =
        scanGeneral
            <@ fun (_: ClArray<'a>) (_: 'a) (_: int) (_: int) (_: int) -> () @>
            <@ fun (resultBuffer: ClArray<'a>) (resultLocalBuffer: 'a []) (inputArrayLength: int) (_: int) (gid: int) (i: int) (localID: int) ->

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

    let private runInPlace (opAdd: Expr<'a -> 'a -> 'a>) (mirror: bool) scan (clContext: ClContext) workGroupSize =

        let scan = scan opAdd clContext workGroupSize

        let scanExclusive =
            scanExclusive opAdd clContext workGroupSize

        let update = update opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (zero: 'a) ->

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

            let totalSum = clContext.CreateClCell<'a>()

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

            firstVertices.Free processor
            secondVertices.Free processor

            totalSum

    let runExcludeInPlace plus = runInPlace plus false scanExclusive

    let runIncludeInPlace plus = runInPlace plus false scanInclusive

    let runBackwardsExcludeInPlace plus = runInPlace plus true scanExclusive

    let runBackwardsIncludeInPlace plus = runInPlace plus true scanInclusive

    /// <summary>
    /// This method is deprecated due to bad perfomance.
    /// Exclude in-place prefix sum of integer array with addition operation and start value that is equal to 0.
    /// </summary>
    /// <example>
    /// <code>
    /// let arr = [| 1; 1; 1; 1 |]
    /// let sum = [| 0 |]
    /// runExcludeInplace clContext workGroupSize processor arr sum (+) 0
    /// |> ignore
    /// ...
    /// > val arr = [| 0; 1; 2; 3 |]
    /// > val sum = [| 4 |]
    /// </code>
    /// </example>
    /// <param name="clContext">ClContext.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let standardExcludeInPlace (clContext: ClContext) workGroupSize =

        let scan =
            runExcludeInPlace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) ->

            scan processor inputArray 0

    /// <summary>
    /// Include in-place prefix sum of integer array with addition operation and start value that is equal to 0.
    /// </summary>
    /// <example>
    /// <code>
    /// let arr = [| 1; 1; 1; 1 |]
    /// let sum = [| 0 |]
    /// runIncludeInplace clContext workGroupSize processor arr sum (+) 0
    /// |> ignore
    /// ...
    /// > val arr = [| 1; 2; 3; 4 |]
    /// > val sum = [| 4 |]
    /// </code>
    /// </example>
    /// <param name="clContext">ClContext.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let standardIncludeInPlace (clContext: ClContext) workGroupSize =

        let scan =
            runIncludeInPlace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) ->

            scan processor inputArray 0

    module ByKey =
        let private sequentialSegments opWrite opAdd zero (clContext: ClContext) workGroupSize =

            let kernel =
                <@ fun (ndRange: Range1D) lenght uniqueKeysCount (values: ClArray<'a>) (keys: ClArray<int>) (offsets: ClArray<int>) ->
                    let gid = ndRange.GlobalID0

                    if gid < uniqueKeysCount then
                        let sourcePosition = offsets.[gid]
                        let sourceKey = keys.[sourcePosition]

                        let mutable currentSum = zero
                        let mutable previousSum = zero

                        let mutable currentPosition = sourcePosition

                        while currentPosition < lenght
                              && keys.[currentPosition] = sourceKey do

                            previousSum <- currentSum
                            currentSum <- (%opAdd) currentSum values.[currentPosition]

                            values.[currentPosition] <- (%opWrite) previousSum currentSum

                            currentPosition <- currentPosition + 1 @>

            let kernel = clContext.Compile kernel

            fun (processor: MailboxProcessor<_>) uniqueKeysCount (values: ClArray<'a>) (keys: ClArray<int>) (offsets: ClArray<int>) ->

                let kernel = kernel.GetKernel()

                let ndRange =
                    Range1D.CreateValid(values.Length, workGroupSize)

                processor.Post(
                    Msg.MsgSetArguments
                        (fun () -> kernel.KernelFunc ndRange values.Length uniqueKeysCount values keys offsets)
                )

                processor.Post(Msg.CreateRunMsg<_, _> kernel)

        /// <summary>
        /// Exclude scan by key.
        /// </summary>
        /// <example>
        /// <code>
        /// let arr = [| 1; 1; 1; 1; 1; 1|]
        /// let keys = [| 1; 2; 2; 2; 3; 3 |]
        /// ...
        /// > val result = [| 0; 0; 1; 2; 0; 1 |]
        /// </code>
        /// </example>
        let sequentialExclude op = sequentialSegments (Map.fst ()) op

        /// <summary>
        /// Include scan by key.
        /// </summary>
        /// <example>
        /// <code>
        /// let arr = [| 1; 1; 1; 1; 1; 1|]
        /// let keys = [| 1; 2; 2; 2; 3; 3 |]
        /// ...
        /// > val result = [| 1; 1; 2; 3; 1; 2 |]
        /// </code>
        /// </example>
        let sequentialInclude op = sequentialSegments (Map.snd ()) op
