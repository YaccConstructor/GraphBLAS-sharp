namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

module PrefixSum =
    let private update mirror (opAdd: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =

        let update =
            <@ fun (ndRange: Range1D) (inputArrayLength: int) (bunchLength: int) (resultBuffer: ClArray<'a>) (verticesBuffer: ClArray<'a>) ->

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

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange inputArrayLength bunchLength inputArray vertices)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private scanGeneral
        beforeLocalSumClear
        writeData
        mirror
        (opAdd: Expr<'a -> 'a -> 'a>)
        zero
        (clContext: ClContext)
        workGroupSize
        =

        let scan =
            <@ fun (ndRange: Range1D) inputArrayLength verticesLength (inputArray: ClArray<'a>) (verticesBuffer: ClArray<'a>) (totalSumBuffer: ClCell<'a>) ->

                let resultLocalBuffer = localArray<'a> workGroupSize
                let mutable i = ndRange.GlobalID0
                let gid = i

                if mirror then
                    i <- inputArrayLength - 1 - i

                let lid = ndRange.LocalID0

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

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (inputArrayLength: int) (vertices: ClArray<'a>) (verticesLength: int) (totalSum: ClCell<'a>) ->

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(inputArrayLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange inputArrayLength verticesLength inputArray vertices totalSum)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

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

    let private runInPlace scan (mirror: bool) (opAdd: Expr<'a -> 'a -> 'a>) zero (clContext: ClContext) workGroupSize =

        let scan =
            scan mirror opAdd zero clContext workGroupSize

        let scanExclusive =
            scanExclusive mirror opAdd zero clContext workGroupSize

        let update =
            update mirror opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let sourceLength =
                (inputArray.Length - 1) / workGroupSize + 1

            let firstVertices =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, sourceLength)

            let secondVertices =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, sourceLength)

            let totalSum = clContext.CreateClCell<'a>()

            let mutable verticesArrays = firstVertices, secondVertices
            let swap (a, b) = (b, a)
            let mutable verticesLength = firstVertices.Length
            let mutable bunchLength = workGroupSize

            scan processor inputArray inputArray.Length (fst verticesArrays) verticesLength totalSum

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

                update processor inputArray inputArray.Length fstVertices bunchLength mirror
                bunchLength <- bunchLength * workGroupSize
                verticesArrays <- swap verticesArrays
                verticesLength <- (verticesLength - 1) / workGroupSize + 1

            firstVertices.Free processor
            secondVertices.Free processor

            totalSum

    let runExcludeInPlace plus = runInPlace scanExclusive false plus

    let runIncludeInPlace plus = runInPlace scanInclusive false plus

    let runBackwardsExcludeInPlace plus = runInPlace scanExclusive true plus

    let runBackwardsIncludeInPlace plus = runInPlace scanInclusive true plus

    /// <summary>
    /// Exclude inplace prefix sum.
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
    ///<param name="clContext">ClContext.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let standardExcludeInPlace (clContext: ClContext) workGroupSize =

        let scan =
            runExcludeInPlace <@ (+) @> 0 clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) ->

            scan processor inputArray

    /// <summary>
    /// Include inplace prefix sum.
    /// </summary>
    /// <example>
    /// <code>
    /// let arr = [| 1; 1; 1; 1 |]
    /// let sum = [| 0 |]
    /// runExcludeInplace clContext workGroupSize processor arr sum (+) 0
    /// |> ignore
    /// ...
    /// > val arr = [| 1; 2; 3; 4 |]
    /// > val sum = [| 4 |]
    /// </code>
    /// </example>
    ///<param name="clContext">ClContext.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let standardIncludeInPlace (clContext: ClContext) workGroupSize =

        let scan =
            runIncludeInPlace <@ (+) @> 0 clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) ->

            scan processor inputArray

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
