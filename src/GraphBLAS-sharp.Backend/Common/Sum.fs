namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClCellExtensions

module Reduce =
    /// <summary>
    /// Generalized reduction pattern.
    /// </summary>
    let private runGeneral (clContext: ClContext) workGroupSize scan scanToCell =

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let scan = scan processor

            let firstLength =
                (inputArray.Length - 1) / workGroupSize + 1

            let firstVerticesArray =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, firstLength)

            let secondLength = (firstLength - 1) / workGroupSize + 1

            let secondVerticesArray =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, secondLength)

            let mutable verticesArrays = firstVerticesArray, secondVerticesArray
            let swap (a, b) = (b, a)

            scan inputArray inputArray.Length (fst verticesArrays)

            let mutable verticesLength = firstLength

            while verticesLength > workGroupSize do
                let fstVertices = fst verticesArrays
                let sndVertices = snd verticesArrays

                scan fstVertices verticesLength sndVertices

                verticesArrays <- swap verticesArrays
                verticesLength <- (verticesLength - 1) / workGroupSize + 1

            let fstVertices = fst verticesArrays

            let result =
                scanToCell processor fstVertices verticesLength

            firstVerticesArray.Free processor
            secondVerticesArray.Free processor

            result

    let private scanSum (opAdd: Expr<'a -> 'a -> 'a>) (clContext: ClContext) (workGroupSize: int) zero =

        let subSum = SubSum.sequentialSum opAdd

        let scan =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (resultArray: ClArray<'a>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let localValues = localArray<'a> workGroupSize

                if gid < length then
                    localValues.[lid] <- inputArray.[gid]
                else
                    localValues.[lid] <- zero

                barrierLocal ()

                (%subSum) workGroupSize lid localValues

                resultArray.[gid / workGroupSize] <- localValues.[0] @>

        let kernel = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>) valuesLength (resultArray: ClArray<'a>) ->
            let ndRange =
                Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange valuesLength valuesArray resultArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private scanToCellSum (opAdd: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize zero =

        let subSum = SubSum.sequentialSum opAdd

        let scan =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (resultCell: ClCell<'a>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let localValues = localArray<'a> workGroupSize

                if gid < length then
                    localValues.[lid] <- inputArray.[gid]
                else
                    localValues.[lid] <- zero

                barrierLocal ()

                (%subSum) workGroupSize lid localValues

                resultCell.Value <- localValues.[0] @>

        let kernel = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>) valuesLength ->

            let ndRange =
                Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let resultCell = clContext.CreateClCell zero

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange valuesLength valuesArray resultCell))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultCell

    /// <summary>
    /// Summarizes array elements.
    /// </summary>
    /// <param name="clContext">ClContext.</param>
    /// <param name="workGroupSize">Work group size.</param>
    /// <param name="op">Summation operation.</param>
    /// <param name="zero">Neutral element for summation.</param>
    let sum op zero (clContext: ClContext) workGroupSize =

        let scan = scanSum op clContext workGroupSize zero

        let scanToCell =
            scanToCellSum op clContext workGroupSize zero

        let run =
            runGeneral clContext workGroupSize scan scanToCell

        fun (processor: MailboxProcessor<_>) (array: ClArray<'a>) -> run processor array

    let private scanReduce<'a when 'a: struct>
        (opAdd: Expr<'a -> 'a -> 'a>)
        (clContext: ClContext)
        (workGroupSize: int)
        =

        let scan =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (resultArray: ClArray<'a>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let localValues = localArray<'a> workGroupSize

                if gid < length then
                    localValues.[lid] <- inputArray.[gid]

                barrierLocal ()

                if gid < length then

                    (%SubReduce.run opAdd) length workGroupSize gid lid localValues

                    if lid = 0 then
                        resultArray.[gid / workGroupSize] <- localValues.[0] @>

        let kernel = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>) valuesLength (resultArray: ClArray<'a>) ->

            let ndRange =
                Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange valuesLength valuesArray resultArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private scanToCellReduce<'a when 'a: struct>
        (opAdd: Expr<'a -> 'a -> 'a>)
        (clContext: ClContext)
        (workGroupSize: int)
        =

        let scan =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (resultValue: ClCell<'a>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let localValues = localArray<'a> workGroupSize

                if gid < length then
                    localValues.[lid] <- inputArray.[gid]

                barrierLocal ()

                if gid < length then

                    (%SubReduce.run opAdd) length workGroupSize gid lid localValues

                    if lid = 0 then
                        resultValue.Value <- localValues.[0] @>

        let kernel = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>) valuesLength ->

            let ndRange =
                Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let resultCell =
                clContext.CreateClCell Unchecked.defaultof<'a>

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange valuesLength valuesArray resultCell))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultCell

    /// <summary>
    /// Reduces an array of values.
    /// </summary>
    /// <param name="clContext">ClContext.</param>
    /// <param name="workGroupSize">Work group size.</param>
    /// <param name="op">Reduction operation.</param>
    let reduce op (clContext: ClContext) workGroupSize =

        let scan = scanReduce op clContext workGroupSize

        let scanToCell =
            scanToCellReduce op clContext workGroupSize

        let run =
            runGeneral clContext workGroupSize scan scanToCell

        fun (processor: MailboxProcessor<_>) (array: ClArray<'a>) -> run processor array

    /// <summary>
    /// Reduction of an array of values by an array of keys.
    /// </summary>
    module ByKey =
        /// <summary>
        /// Reduces an array of values by key using a single work item.
        /// </summary>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Work group size.</param>
        /// <param name="reduceOp">Operation for reducing values.</param>
        /// <remarks>
        /// The length of the result must be calculated in advance.
        /// </remarks>
        let sequential (reduceOp: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =

            let kernel =
                <@ fun (ndRange: Range1D) length (keys: ClArray<int>) (values: ClArray<'a>) (reducedValues: ClArray<'a>) (reducedKeys: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid = 0 then
                        let mutable currentKey = keys.[0]
                        let mutable segmentResult = values.[0]
                        let mutable segmentCount = 0

                        for i in 1 .. length - 1 do
                            if currentKey = keys.[i] then
                                segmentResult <- (%reduceOp) segmentResult values.[i]
                            else
                                reducedValues.[segmentCount] <- segmentResult
                                reducedKeys.[segmentCount] <- currentKey

                                segmentCount <- segmentCount + 1
                                currentKey <- keys.[i]
                                segmentResult <- values.[i]

                        reducedKeys.[segmentCount] <- currentKey
                        reducedValues.[segmentCount] <- segmentResult @>

            let kernel = clContext.Compile kernel

            fun (processor: MailboxProcessor<_>) allocationMode (resultLength: int) (keys: ClArray<int>) (values: ClArray<'a>) ->

                let reducedValues =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let reducedKeys =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let ndRange =
                    Range1D.CreateValid(resultLength, workGroupSize)

                let kernel = kernel.GetKernel()

                processor.Post(
                    Msg.MsgSetArguments
                        (fun () -> kernel.KernelFunc ndRange keys.Length keys values reducedValues reducedKeys)
                )

                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

                reducedValues, reducedKeys

        /// <summary>
        /// Reduces values by key. Each segment is reduced by one work item.
        /// </summary>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Work group size.</param>
        /// <param name="reduceOp">Operation for reducing values.</param>
        /// <remarks>
        /// The length of the result must be calculated in advance.
        /// </remarks>
        let segmentSequential (reduceOp: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =

            let kernel =
                <@ fun (ndRange: Range1D) uniqueKeyCount keysLength (offsets: ClArray<int>) (keys: ClArray<int>) (values: ClArray<'a>) (reducedValues: ClArray<'a>) (reducedKeys: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid < uniqueKeyCount then
                        let startPosition = offsets.[gid]

                        let sourceKey = keys.[startPosition]
                        let mutable sum = values.[startPosition]

                        let mutable currentPosition = startPosition + 1

                        while currentPosition < keysLength
                              && sourceKey = keys.[currentPosition] do

                            sum <- (%reduceOp) sum values.[currentPosition]
                            currentPosition <- currentPosition + 1

                        reducedValues.[gid] <- sum
                        reducedKeys.[gid] <- sourceKey @>

            let kernel = clContext.Compile kernel

            fun (processor: MailboxProcessor<_>) allocationMode (resultLength: int) (offsets: ClArray<int>) (keys: ClArray<int>) (values: ClArray<'a>) ->

                let reducedValues =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let reducedKeys =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let ndRange =
                    Range1D.CreateValid(resultLength, workGroupSize)

                let kernel = kernel.GetKernel()

                processor.Post(
                    Msg.MsgSetArguments
                        (fun () ->
                            kernel.KernelFunc
                                ndRange
                                resultLength
                                keys.Length
                                offsets
                                keys
                                values
                                reducedValues
                                reducedKeys)
                )

                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

                reducedValues, reducedKeys

        /// <summary>
        /// Reduces values by key. One work group participates in the reduction.
        /// </summary>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Work group size.</param>
        /// <param name="reduceOp">Operation for reducing values.</param>
        /// <remarks>
        /// Reduces an array of values that does not exceed the size of the workgroup.
        /// The length of the result must be calculated in advance.
        /// </remarks>
        let oneWorkGroupSegments (reduceOp: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =

            let kernel =
                <@ fun (ndRange: Range1D) length (keys: ClArray<int>) (values: ClArray<'a>) (reducedValues: ClArray<'a>) (reducedKeys: ClArray<int>) ->

                    let lid = ndRange.GlobalID0

                    // load values to local memory (may be without it)
                    let localValues = localArray<'a> workGroupSize

                    if lid < length then
                        localValues.[lid] <- values.[lid]

                    // load keys to local memory (mb without it)
                    let localKeys = localArray<int> workGroupSize

                    if lid < length then
                        localKeys.[lid] <- keys.[lid]

                    // get unique keys bitmap
                    let localBitmap = localArray<int> workGroupSize
                    localBitmap.[lid] <- 0
                    (%PreparePositions.getUniqueBitmapLocal<int>) localKeys workGroupSize lid localBitmap

                    // get positions from bitmap by prefix sum
                    // ??? get bitmap by prefix sum in another kernel ???
                    // ??? we can restrict prefix sum for 0 .. length ???
                    (%SubSum.localIntPrefixSum) lid workGroupSize localBitmap

                    let uniqueKeysCount = localBitmap.[length - 1]

                    if lid < uniqueKeysCount then
                        let itemKeyId = lid + 1

                        let startKeyIndex =
                            (%Search.Bin.lowerPositionLocal) length itemKeyId localBitmap

                        match startKeyIndex with
                        | Some startPosition ->
                            let sourceKeyPosition = localBitmap.[startPosition]
                            let mutable currentSum = localValues.[startPosition]
                            let mutable currentIndex = startPosition + 1

                            while currentIndex < length
                                  && localBitmap.[currentIndex] = sourceKeyPosition do

                                currentSum <- (%reduceOp) currentSum localValues.[currentIndex]
                                currentIndex <- currentIndex + 1

                            reducedKeys.[lid] <- localKeys.[startPosition]
                            reducedValues.[lid] <- currentSum
                        | None -> () @>

            let kernel = clContext.Compile kernel

            fun (processor: MailboxProcessor<_>) allocationMode (resultLength: int) (keys: ClArray<int>) (values: ClArray<'a>) ->
                if keys.Length > workGroupSize then
                    failwith "The length of the value should not exceed the size of the workgroup"

                let reducedValues =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let reducedKeys =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let ndRange =
                    Range1D.CreateValid(resultLength, workGroupSize)

                let kernel = kernel.GetKernel()

                processor.Post(
                    Msg.MsgSetArguments
                        (fun () -> kernel.KernelFunc ndRange keys.Length keys values reducedValues reducedKeys)
                )

                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

                reducedValues, reducedKeys

        module Option =
            /// <summary>
            /// Reduces values by key. Each segment is reduced by one work item.
            /// </summary>
            /// <param name="clContext">ClContext.</param>
            /// <param name="workGroupSize">Work group size.</param>
            /// <param name="reduceOp">Operation for reducing values.</param>
            let segmentSequential<'a>
                (reduceOp: Expr<'a option -> 'a option -> 'a option>)
                (clContext: ClContext)
                workGroupSize
                =

                let kernel =
                    <@ fun (ndRange: Range1D) uniqueKeyCount keysLength (offsets: ClArray<int>) (keys: ClArray<int>) (values: ClArray<'a option>) (reducedValues: ClArray<'a>) (firstReducedKeys: ClArray<int>) (resultPositions: ClArray<int>) ->

                        let gid = ndRange.GlobalID0

                        if gid < uniqueKeyCount then
                            let startPosition =
                                (%Search.Bin.lowerPosition) keysLength gid offsets

                            match startPosition with
                            | Some startPosition ->
                                let firstSourceKey = keys.[startPosition]

                                let mutable sum = None

                                let mutable currentPosition = startPosition

                                while currentPosition < keysLength
                                      && firstSourceKey = keys.[currentPosition] do
                                    let result = (%reduceOp) sum values.[currentPosition] // brahma error
                                    sum <- result
                                    currentPosition <- currentPosition + 1

                                match sum with
                                | Some value ->
                                    reducedValues.[gid] <- value
                                    resultPositions.[gid] <- 1
                                | None -> resultPositions.[gid] <- 0

                                firstReducedKeys.[gid] <- firstSourceKey
                            | None -> () @> // not possible if done correctly

                let kernel = clContext.Compile kernel

                let getUniqueBitmap =
                    Bitmap.lastOccurrence clContext workGroupSize

                let scatterData =
                    Scatter.lastOccurrence clContext workGroupSize

                let scatterIndices =
                    Scatter.lastOccurrence clContext workGroupSize

                let prefixSum =
                    PrefixSumInternal.standardExcludeInPlace clContext workGroupSize

                fun (processor: MailboxProcessor<_>) allocationMode (keys: ClArray<int>) (values: ClArray<'a option>) ->

                    let offsets =
                        getUniqueBitmap processor DeviceOnly keys

                    let uniqueKeysCount =
                        (prefixSum processor offsets)
                            .ToHostAndFree processor

                    let reducedValues =
                        clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, uniqueKeysCount)

                    let reducedKeys =
                        clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, uniqueKeysCount)

                    let resultPositions =
                        clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, uniqueKeysCount)

                    let ndRange =
                        Range1D.CreateValid(uniqueKeysCount, workGroupSize)

                    let kernel = kernel.GetKernel()

                    processor.Post(
                        Msg.MsgSetArguments
                            (fun () ->
                                kernel.KernelFunc
                                    ndRange
                                    uniqueKeysCount
                                    keys.Length
                                    offsets
                                    keys
                                    values
                                    reducedValues
                                    reducedKeys
                                    resultPositions)
                    )

                    processor.Post(Msg.CreateRunMsg<_, _>(kernel))

                    offsets.Free processor

                    let resultLength =
                        (prefixSum processor resultPositions)
                            .ToHostAndFree processor

                    if resultLength = 0 then
                        reducedValues.Free processor
                        reducedKeys.Free processor
                        resultPositions.Free processor
                        None
                    else
                        // write values
                        let resultValues =
                            clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                        scatterData processor resultPositions reducedValues resultValues

                        reducedValues.Free processor

                        // write keys
                        let resultKeys =
                            clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                        scatterIndices processor resultPositions reducedKeys resultKeys

                        reducedKeys.Free processor
                        resultPositions.Free processor

                        Some(resultValues, resultKeys)

            /// <summary>
            /// Reduces values by key. Each segment is reduced by one work item.
            /// </summary>
            /// <param name="clContext">ClContext.</param>
            /// <param name="workGroupSize">Work group size.</param>
            /// <param name="reduceOp">Operation for reducing values.</param>
            /// <remarks>
            /// The length of the result and offsets for each segment must be calculated in advance.
            /// </remarks>
            let segmentSequentialByOffsets<'a>
                (reduceOp: Expr<'a -> 'a -> 'a option>)
                (clContext: ClContext)
                workGroupSize
                =

                let kernel =
                    <@ fun (ndRange: Range1D) uniqueKeyCount keysLength (offsets: ClArray<int>) (keys: ClArray<int>) (values: ClArray<'a>) (reducedValues: ClArray<'a>) (firstReducedKeys: ClArray<int>) (resultPositions: ClArray<int>) ->

                        let gid = ndRange.GlobalID0

                        if gid < uniqueKeyCount then
                            let startPosition = offsets.[gid]

                            let firstSourceKey = keys.[startPosition]

                            let mutable sum = Some values.[startPosition]

                            let mutable currentPosition = startPosition + 1

                            while currentPosition < keysLength
                                  && firstSourceKey = keys.[currentPosition] do

                                match sum with
                                | Some value ->
                                    let result =
                                        ((%reduceOp) value values.[currentPosition]) // brahma error

                                    sum <- result
                                | None -> sum <- Some values.[currentPosition]

                                currentPosition <- currentPosition + 1

                            match sum with
                            | Some value ->
                                reducedValues.[gid] <- value
                                resultPositions.[gid] <- 1
                            | None -> resultPositions.[gid] <- 0

                            firstReducedKeys.[gid] <- firstSourceKey @>

                let kernel = clContext.Compile kernel

                let scatterData =
                    Scatter.lastOccurrence clContext workGroupSize

                let scatterIndices =
                    Scatter.lastOccurrence clContext workGroupSize

                let prefixSum =
                    PrefixSumInternal.standardExcludeInPlace clContext workGroupSize

                fun (processor: MailboxProcessor<_>) allocationMode (resultLength: int) (offsets: ClArray<int>) (keys: ClArray<int>) (values: ClArray<'a>) ->

                    let reducedValues =
                        clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

                    let reducedKeys =
                        clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

                    let resultPositions =
                        clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

                    let ndRange =
                        Range1D.CreateValid(resultLength, workGroupSize)

                    let kernel = kernel.GetKernel()

                    processor.Post(
                        Msg.MsgSetArguments
                            (fun () ->
                                kernel.KernelFunc
                                    ndRange
                                    resultLength
                                    keys.Length
                                    offsets
                                    keys
                                    values
                                    reducedValues
                                    reducedKeys
                                    resultPositions)
                    )

                    processor.Post(Msg.CreateRunMsg<_, _>(kernel))

                    let resultLength =
                        (prefixSum processor resultPositions)
                            .ToHostAndFree processor

                    if resultLength = 0 then
                        reducedValues.Free processor
                        reducedKeys.Free processor
                        resultPositions.Free processor

                        None
                    else
                        // write values
                        let resultValues =
                            clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                        scatterData processor resultPositions reducedValues resultValues

                        reducedValues.Free processor

                        // write keys
                        let resultKeys =
                            clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                        scatterIndices processor resultPositions reducedKeys resultKeys

                        reducedKeys.Free processor
                        resultPositions.Free processor

                        Some(resultValues, resultKeys)

    module ByKey2D =
        /// <summary>
        /// Reduces an array of values by 2D keys using a single work item.
        /// </summary>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Work group size.</param>
        /// <param name="reduceOp">Operation for reducing values.</param>
        /// <remarks>
        /// The length of the result must be calculated in advance.
        /// </remarks>
        let sequential (reduceOp: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =

            let kernel =
                <@ fun (ndRange: Range1D) length (firstKeys: ClArray<int>) (secondKeys: ClArray<int>) (values: ClArray<'a>) (reducedValues: ClArray<'a>) (firstReducedKeys: ClArray<int>) (secondReducedKeys: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid = 0 then
                        let mutable firstCurrentKey = firstKeys.[0]
                        let mutable secondCurrentKey = secondKeys.[0]

                        let mutable segmentResult = values.[0]
                        let mutable segmentCount = 0

                        for i in 1 .. length - 1 do
                            if firstCurrentKey = firstKeys.[i]
                               && secondCurrentKey = secondKeys.[i] then
                                segmentResult <- (%reduceOp) segmentResult values.[i]
                            else
                                reducedValues.[segmentCount] <- segmentResult

                                firstReducedKeys.[segmentCount] <- firstCurrentKey
                                secondReducedKeys.[segmentCount] <- secondCurrentKey

                                segmentCount <- segmentCount + 1
                                firstCurrentKey <- firstKeys.[i]
                                secondCurrentKey <- secondKeys.[i]
                                segmentResult <- values.[i]

                        firstReducedKeys.[segmentCount] <- firstCurrentKey
                        secondReducedKeys.[segmentCount] <- secondCurrentKey

                        reducedValues.[segmentCount] <- segmentResult @>

            let kernel = clContext.Compile kernel

            fun (processor: MailboxProcessor<_>) allocationMode (resultLength: int) (firstKeys: ClArray<int>) (secondKeys: ClArray<int>) (values: ClArray<'a>) ->

                let reducedValues =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let firstReducedKeys =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let secondReducedKeys =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let ndRange =
                    Range1D.CreateValid(resultLength, workGroupSize)

                let kernel = kernel.GetKernel()

                processor.Post(
                    Msg.MsgSetArguments
                        (fun () ->
                            kernel.KernelFunc
                                ndRange
                                firstKeys.Length
                                firstKeys
                                secondKeys
                                values
                                reducedValues
                                firstReducedKeys
                                secondReducedKeys)
                )

                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

                reducedValues, firstReducedKeys, secondReducedKeys

        /// <summary>
        /// Reduces values by key. Each segment is reduced by one work item.
        /// </summary>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Work group size.</param>
        /// <param name="reduceOp">Operation for reducing values.</param>
        /// <remarks>
        /// The length of the result must be calculated in advance.
        /// </remarks>
        let segmentSequential<'a> (reduceOp: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =

            let kernel =
                <@ fun (ndRange: Range1D) uniqueKeyCount keysLength (offsets: ClArray<int>) (firstKeys: ClArray<int>) (secondKeys: ClArray<int>) (values: ClArray<'a>) (reducedValues: ClArray<'a>) (firstReducedKeys: ClArray<int>) (secondReducedKeys: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid < uniqueKeyCount then
                        let startPosition = offsets.[gid]

                        let firstSourceKey = firstKeys.[startPosition]
                        let secondSourceKey = secondKeys.[startPosition]

                        let mutable sum = values.[startPosition]

                        let mutable currentPosition = startPosition + 1

                        while currentPosition < keysLength
                              && firstSourceKey = firstKeys.[currentPosition]
                              && secondSourceKey = secondKeys.[currentPosition] do

                            sum <- (%reduceOp) sum values.[currentPosition]
                            currentPosition <- currentPosition + 1

                        reducedValues.[gid] <- sum
                        firstReducedKeys.[gid] <- firstSourceKey
                        secondReducedKeys.[gid] <- secondSourceKey @>

            let kernel = clContext.Compile kernel

            fun (processor: MailboxProcessor<_>) allocationMode (resultLength: int) (offsets: ClArray<int>) (firstKeys: ClArray<int>) (secondKeys: ClArray<int>) (values: ClArray<'a>) ->

                let reducedValues =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let firstReducedKeys =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let secondReducedKeys =
                    clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                let ndRange =
                    Range1D.CreateValid(resultLength, workGroupSize)

                let kernel = kernel.GetKernel()

                processor.Post(
                    Msg.MsgSetArguments
                        (fun () ->
                            kernel.KernelFunc
                                ndRange
                                resultLength
                                firstKeys.Length
                                offsets
                                firstKeys
                                secondKeys
                                values
                                reducedValues
                                firstReducedKeys
                                secondReducedKeys)
                )

                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

                reducedValues, firstReducedKeys, secondReducedKeys

        module Option =
            /// <summary>
            /// Reduces values by key. Each segment is reduced by one work item.
            /// </summary>
            /// <param name="clContext">ClContext.</param>
            /// <param name="workGroupSize">Work group size.</param>
            /// <param name="reduceOp">Operation for reducing values.</param>
            /// <remarks>
            /// The length of the result must be calculated in advance.
            /// </remarks>
            let segmentSequential<'a> (reduceOp: Expr<'a -> 'a -> 'a option>) (clContext: ClContext) workGroupSize =

                let kernel =
                    <@ fun (ndRange: Range1D) uniqueKeyCount keysLength (offsets: ClArray<int>) (firstKeys: ClArray<int>) (secondKeys: ClArray<int>) (values: ClArray<'a>) (reducedValues: ClArray<'a>) (firstReducedKeys: ClArray<int>) (secondReducedKeys: ClArray<int>) (resultPositions: ClArray<int>) ->

                        let gid = ndRange.GlobalID0

                        if gid < uniqueKeyCount then
                            let startPosition = offsets.[gid]

                            let firstSourceKey = firstKeys.[startPosition]
                            let secondSourceKey = secondKeys.[startPosition]

                            let mutable sum = Some values.[startPosition]

                            let mutable currentPosition = startPosition + 1

                            while currentPosition < keysLength
                                  && firstSourceKey = firstKeys.[currentPosition]
                                  && secondSourceKey = secondKeys.[currentPosition] do

                                match sum with
                                | Some value ->
                                    let result =
                                        ((%reduceOp) value values.[currentPosition]) // brahma error

                                    sum <- result
                                | None -> sum <- Some values.[currentPosition]

                                currentPosition <- currentPosition + 1

                            match sum with
                            | Some value ->
                                reducedValues.[gid] <- value
                                resultPositions.[gid] <- 1
                            | None -> resultPositions.[gid] <- 0

                            firstReducedKeys.[gid] <- firstSourceKey
                            secondReducedKeys.[gid] <- secondSourceKey @>

                let kernel = clContext.Compile kernel

                let scatterData =
                    Scatter.lastOccurrence clContext workGroupSize

                let scatterIndices =
                    Scatter.lastOccurrence clContext workGroupSize

                let prefixSum =
                    PrefixSumInternal.standardExcludeInPlace clContext workGroupSize

                fun (processor: MailboxProcessor<_>) allocationMode (resultLength: int) (offsets: ClArray<int>) (firstKeys: ClArray<int>) (secondKeys: ClArray<int>) (values: ClArray<'a>) ->

                    let reducedValues =
                        clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                    let firstReducedKeys =
                        clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                    let secondReducedKeys =
                        clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                    let resultPositions =
                        clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, resultLength)

                    let ndRange =
                        Range1D.CreateValid(resultLength, workGroupSize)

                    let kernel = kernel.GetKernel()

                    processor.Post(
                        Msg.MsgSetArguments
                            (fun () ->
                                kernel.KernelFunc
                                    ndRange
                                    resultLength
                                    firstKeys.Length
                                    offsets
                                    firstKeys
                                    secondKeys
                                    values
                                    reducedValues
                                    firstReducedKeys
                                    secondReducedKeys
                                    resultPositions)
                    )

                    processor.Post(Msg.CreateRunMsg<_, _>(kernel))

                    let resultLength =
                        (prefixSum processor resultPositions)
                            .ToHostAndFree processor

                    if resultLength = 0 then
                        reducedValues.Free processor
                        firstReducedKeys.Free processor
                        secondReducedKeys.Free processor
                        resultPositions.Free processor

                        None
                    else
                        // write value
                        let resultValues =
                            clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                        scatterData processor resultPositions reducedValues resultValues

                        reducedValues.Free processor

                        // write first keys
                        let resultFirstKeys =
                            clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                        scatterIndices processor resultPositions firstReducedKeys resultFirstKeys

                        firstReducedKeys.Free processor

                        // write second keys
                        let resultSecondKeys =
                            clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, resultLength)

                        scatterIndices processor resultPositions secondReducedKeys resultSecondKeys

                        secondReducedKeys.Free processor

                        resultPositions.Free processor

                        Some(resultValues, resultFirstKeys, resultSecondKeys)
