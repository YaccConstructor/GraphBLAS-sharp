namespace GraphBLAS.FSharp.Backend

open System
open System.Diagnostics.CodeAnalysis
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module EWise =
    let private getResRowSizes (clContext: ClContext) workGroupSize =

        let getResRowSizes =
            <@ fun (ndRange: Range1D) (leftRowPointers: ClArray<int>) (rightRowPointers: ClArray<int>) (resRowSizes: ClArray<int>) rows leftNNZ rightNNZ ->

                let row = ndRange.globalID0

                if row < rows then

                    let leftOffset = leftRowPointers.[row]
                    let rightOffset = rightRowPointers.[row]
                    let leftBorder = if row = rows - 1 then leftNNZ else leftRowPointers.[row + 1]
                    let rightBorder = if row = rows - 1 then rightNNZ else rightRowPointers.[row + 1]

                    let leftRowSize = leftBorder - leftOffset
                    let rightRowSize = rightBorder - rightOffset

                    resRowSizes.[row] <- leftRowSize + rightRowSize
                 @>

        let kernel = clContext.Compile(getResRowSizes)

        fun (processor: MailboxProcessor<_>) (leftRowPointers: ClArray<int>) (rightRowPointers: ClArray<int>) rows leftNNZ rightNNZ ->

            let ndRange =
                Range1D.CreateValid(leftRowPointers.Length, workGroupSize)

            let resRowSizes =
                clContext.CreateClArray(
                    leftRowPointers.Length
                )

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange leftRowPointers rightRowPointers resRowSizes rows leftNNZ rightNNZ))

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            resRowSizes


    let private copyWithOffset (clContext: ClContext) workGroupSize =

        let copyWithOffsetLeft =
            <@ fun (ndRange: Range1D) rowSize srcOffset dstOffset (sourceCols: ClArray<int>) (sourceValues: ClArray<'a>) (destinationCols: ClArray<int>) (destinationValues: ClArray<'a>) (isEndOfRowBitmap: ClArray<int>) (isLeftBitmap: ClArray<int>) ->

                    let i = ndRange.globalID0

                    if i < rowSize
                    then
                        destinationCols.[dstOffset + i] <- sourceCols.[srcOffset + i]
                        destinationValues.[dstOffset + i] <- sourceValues.[srcOffset + i]
                        isLeftBitmap.[dstOffset + i] <- 1
                        isEndOfRowBitmap.[dstOffset + i] <- if i = rowSize - 1 then 1 else 0 @>

        let copyWithOffsetRight =
            <@ fun (ndRange: Range1D) rowSize srcOffset dstOffset (sourceCols: ClArray<int>) (sourceValues: ClArray<'a>) (destinationCols: ClArray<int>) (destinationValues: ClArray<'a>) (isEndOfRowBitmap: ClArray<int>) (isLeftBitmap: ClArray<int>) ->

                    let i = ndRange.globalID0

                    if i < rowSize
                    then
                        destinationCols.[dstOffset + i] <- sourceCols.[srcOffset + i]
                        destinationValues.[dstOffset + i] <- sourceValues.[srcOffset + i]
                        isLeftBitmap.[dstOffset + i] <- 0
                        isEndOfRowBitmap.[dstOffset + i] <- if i = rowSize - 1 then 1 else 0 @>

        let kernelLeft = clContext.Compile(copyWithOffsetLeft)
        let kernelRight = clContext.Compile(copyWithOffsetRight)

        fun (processor: MailboxProcessor<_>) rowSize srcOffset dstOffset isLeft (sourceCols: ClArray<int>) (sourceValues: ClArray<'a>) (destinationCols: ClArray<int>) (destinationValues: ClArray<'a>) (isEndOfRowBitmap: ClArray<int>) (isLeftBitmap: ClArray<int>) ->

            let ndRange = Range1D.CreateValid(rowSize, workGroupSize)
            let kernel = if isLeft then kernelLeft.GetKernel() else kernelRight.GetKernel()

            processor.Post(
                        Msg.MsgSetArguments
                            (fun () ->
                                kernel.KernelFunc
                                    ndRange
                                    rowSize
                                    srcOffset
                                    dstOffset
                                    sourceCols
                                    sourceValues
                                    destinationCols
                                    destinationValues
                                    isEndOfRowBitmap
                                    isLeftBitmap)
                    )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private getUniqueBitmap (clContext: ClContext) workGroupSize =

        let getUniqueBitmap =
            <@ fun (ndRange: Range1D) (inputArray: ClArray<'a>) inputLength (isEndOfRowBitmap: ClArray<int>) (isUniqueBitmap: ClArray<int>) ->

                let i = ndRange.globalID0

                if i < inputLength - 1
                   && inputArray.[i] = inputArray.[i + 1]
                   && isEndOfRowBitmap.[i] = 0 then
                    isUniqueBitmap.[i] <- 0
                else
                    isUniqueBitmap.[i] <- 1 @>

        let kernel = clContext.Compile(getUniqueBitmap)

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (isEndOfRow: ClArray<int>) ->

            let inputLength = inputArray.Length

            let ndRange =
                Range1D.CreateValid(inputLength, workGroupSize)

            let bitmap =
                clContext.CreateClArray(
                    inputLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray inputLength isEndOfRow bitmap))

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            bitmap

    let private preparePositions<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let preparePositions =
            <@ fun (ndRange: Range1D) length (allColumns: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (allValues: ClArray<'c>) (rawPositions: ClArray<int>) (isEndOfRowBitmap: ClArray<int>) (isLeftBitmap: ClArray<int>) ->

                let i = ndRange.globalID0

                if (i < length - 1
                    && allColumns.[i] = allColumns.[i + 1]
                    && isEndOfRowBitmap.[i] = 0)
                    then
                    rawPositions.[i] <- 0

                    match (%opAdd) (Some leftValues.[i + 1]) (Some rightValues.[i]) with
                    | Some v ->
                        allValues.[i + 1] <- v
                        rawPositions.[i + 1] <- 1
                    | None -> rawPositions.[i + 1] <- 0
                elif (i > 0
                         && i < length
                         && (allColumns.[i] <> allColumns.[i - 1])
                         || isEndOfRowBitmap.[i - 1] = 1)
                        || i = 0 then
                    if isLeftBitmap.[i] = 1 then
                        match (%opAdd) (Some leftValues.[i]) None with
                        | Some v ->
                            allValues.[i] <- v
                            rawPositions.[i] <- 1
                        | None -> rawPositions.[i] <- 0
                    else
                        match (%opAdd) None (Some rightValues.[i]) with
                        | Some v ->
                            allValues.[i] <- v
                            rawPositions.[i] <- 1
                        | None -> rawPositions.[i] <- 0 @>

        let kernel = clContext.Compile(preparePositions)

        fun (processor: MailboxProcessor<_>) (allColumns: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isEndOfRow: ClArray<int>) (isLeft: ClArray<int>) ->
            let length = leftValues.Length

            let ndRange =
                Range1D.CreateValid(length, workGroupSize)

            let rawPositionsGpu =
                clContext.CreateClArray<int>(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let allValues =
                clContext.CreateClArray<'c>(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            length
                            allColumns
                            leftValues
                            rightValues
                            allValues
                            rawPositionsGpu
                            isEndOfRow
                            isLeft)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
            rawPositionsGpu, allValues

    let private setPositions<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let setPositions =
            <@ fun (ndRange: Range1D) prefixSumArrayLength (allColumns: ClArray<int>) (allValues: ClArray<'a>) (prefixSumArray: ClArray<int>) (resultColumns: ClArray<int>) (resultValues: ClArray<'a>) ->

                let i = ndRange.globalID0

                if i = prefixSumArrayLength - 1
                   || i < prefixSumArrayLength
                      && prefixSumArray.[i]
                         <> prefixSumArray.[i + 1] then
                    let index = prefixSumArray.[i]

                    resultColumns.[index] <- allColumns.[i]
                    resultValues.[index] <- allValues.[i] @>

        let kernel = clContext.Compile(setPositions)

        let sum =
            GraphBLAS.FSharp.Backend.ClArray.prefixSumExcludeInplace clContext workGroupSize

        let resultLength = Array.zeroCreate 1

        fun (processor: MailboxProcessor<_>) (allColumns: ClArray<int>) (allValues: ClArray<'a>) (positions: ClArray<int>) ->
            let prefixSumArrayLength = positions.Length

            let resultLengthGpu = clContext.CreateClCell 0

            let _, r = sum processor positions resultLengthGpu

            let resultLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            let resultColumns =
                clContext.CreateClArray<int>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let resultValues =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let ndRange =
                Range1D.CreateValid(positions.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            prefixSumArrayLength
                            allColumns
                            allValues
                            positions
                            resultColumns
                            resultValues)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultColumns, resultValues, resultLength

    let merge<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) workGroupSize =

        let merge =
            <@ fun
                (ndRange: Range1D)
                rows
                firstNNZ
                secondNNZ
                (resRowSizes: ClArray<int>)
                (firstRowPointers: ClArray<int>)
                (firstColumns: ClArray<int>)
                (firstValues: ClArray<'a>)
                (secondRowPointers: ClArray<int>)
                (secondColumns: ClArray<int>)
                (secondValues: ClArray<'b>)
                (allColumns: ClArray<int>)
                (leftMergedValues: ClArray<'a>)
                (rightMergedValues: ClArray<'b>)
                (isEndOfRowBitmap: ClArray<int>)
                (isLeftBitmap: ClArray<int>) ->

                    let globalID = ndRange.GlobalID0

                    let row = globalID / workGroupSize

                    let firstOffset = firstRowPointers.[row]
                    let secondOffset = secondRowPointers.[row]
                    let resOffset = firstOffset + secondOffset

                    let firstRowEnd = if row = rows - 1 then firstNNZ else firstRowPointers.[row + 1]
                    let secondRowEnd = if row = rows - 1 then secondNNZ else secondRowPointers.[row + 1]

                    let firstRowLength = firstRowEnd - firstOffset
                    let secondRowLength = secondRowEnd - secondOffset
                    let resRowLength = firstRowLength + secondRowLength

                    let workGroupCount = (resRowLength + workGroupSize - 1) / workGroupSize

                    let mutable beginFirst = 0
                    let mutable beginSecond = 0

                    let mutable maxFirstIndex = local ()
                    let mutable maxSecondIndex = local ()

                    //Cycle on each work group for one row
                    for group in 0 .. workGroupCount do

                        let mutable maxFirstIndexPerThread = local ()
                        let mutable maxSecondIndexPerThread = local ()

                        let mutable beginIdxLocal = local ()
                        let mutable endIdxLocal = local ()
                        let localID = ndRange.LocalID0

                        //Binary search for indices of window's beginning and ending
                        //localID = 0 searches for beginning
                        if localID < 2 then
                            let mutable x = localID * (workGroupSize - 1) + globalID - 1

                            if x >= resRowLength then
                                x <- resRowLength - 1

                            let diagonalNumber = x

                            let mutable leftEdge = diagonalNumber + 1 - secondRowLength
                            if leftEdge < 0 then leftEdge <- 0

                            let mutable rightEdge = firstRowLength - 1

                            if rightEdge > diagonalNumber then
                                rightEdge <- diagonalNumber

                            while leftEdge <= rightEdge do
                                let middleIdx = (leftEdge + rightEdge) / 2

                                let firstIndex: uint64 = (uint64 firstColumns.[firstOffset + middleIdx])

                                let secondIndex: uint64 = (uint64 secondColumns.[secondOffset + diagonalNumber - middleIdx])

                                if firstIndex < secondIndex then
                                    leftEdge <- middleIdx + 1
                                else
                                    rightEdge <- middleIdx - 1

                            // Here localID equals either 0 or 1
                            if localID = 0 then
                                beginIdxLocal <- leftEdge
                            else
                                endIdxLocal <- leftEdge

                        barrierLocal ()

                        let beginIdx = beginIdxLocal
                        let endIdx = endIdxLocal
                        let firstLocalLength = endIdx - beginIdx
                        let mutable x = workGroupSize - firstLocalLength

                        if endIdx = firstRowLength then
                            x <- secondRowLength - globalID + localID + beginIdx

                        let secondLocalLength = x

                        //Loading column indices to local memory
                        //First indices are from 0 to firstLocalLength - 1 inclusive
                        //Second indices are from firstLocalLength to firstLocalLength + secondLocalLength - 1 inclusive
                        let localIndices = localArray<uint64> workGroupSize

                        if localID < firstLocalLength then
                            localIndices.[localID] <- (uint64 firstColumns.[firstOffset + beginIdx + localID])

                        if localID < secondLocalLength then
                            localIndices.[firstLocalLength + localID] <- (uint64 secondColumns.[secondOffset + globalID - beginIdx])

                        barrierLocal ()

                        if globalID < resRowLength then
                            let mutable leftEdge = localID + 1 - secondLocalLength
                            if leftEdge < 0 then leftEdge <- 0

                            let mutable rightEdge = firstLocalLength - 1

                            if rightEdge > localID then
                                rightEdge <- localID

                            while leftEdge <= rightEdge do
                                let middleIdx = (leftEdge + rightEdge) / 2
                                let firstIndex = localIndices.[middleIdx]

                                let secondIndex =
                                    localIndices.[firstLocalLength + localID - middleIdx]

                                if firstIndex < secondIndex then
                                    leftEdge <- middleIdx + 1
                                else
                                    rightEdge <- middleIdx - 1

                            let boundaryX = rightEdge
                            let boundaryY = localID - leftEdge

                            // boundaryX and boundaryY can't be off the right edge of array (only off the left edge)
                            let isValidX = boundaryX >= 0
                            let isValidY = boundaryY >= 0

                            let mutable fstIdx = 0UL

                            if isValidX then
                                fstIdx <- localIndices.[boundaryX]

                            let mutable sndIdx = 0UL

                            if isValidY then
                                sndIdx <- localIndices.[firstLocalLength + boundaryY]

                            isEndOfRowBitmap.[firstOffset + secondOffset + globalID] <- if globalID = resRowLength - 1 then 1 else 0

                            if not isValidX || isValidY && fstIdx < sndIdx then
                                allColumns.[firstOffset + secondOffset + globalID] <- int sndIdx
                                rightMergedValues.[firstOffset + secondOffset + globalID] <- secondValues.[secondOffset + globalID - localID - beginIdx + boundaryY]
                                isLeftBitmap.[firstOffset + secondOffset + globalID] <- 0
                            else
                                allColumns.[firstOffset + secondOffset + globalID] <- int fstIdx
                                leftMergedValues.[firstOffset + secondOffset + globalID] <- firstValues.[firstOffset + beginIdx + boundaryX]
                                isLeftBitmap.[firstOffset + secondOffset + globalID] <- 1 @>

//        let copyWithOffsetLeft = copyWithOffset clContext workGroupSize
//        let copyWithOffsetRight = copyWithOffset clContext workGroupSize
        let getResRowSizes = getResRowSizes clContext workGroupSize

        let kernel = clContext.Compile(merge)

        fun (processor: MailboxProcessor<_>) (matrixLeftRowPointers: ClArray<int>) (matrixLeftColumns: ClArray<int>) (matrixLeftValues: ClArray<'a>) (matrixRightRowPointers: ClArray<int>) (matrixRightColumns: ClArray<int>) (matrixRightValues: ClArray<'b>) ->

            let firstLength = matrixLeftValues.Length
            let secondLength = matrixRightValues.Length
            let resLength = firstLength + secondLength

            let allColumns =
                clContext.CreateClArray<int>(
                    resLength,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let leftMergedValues =
                clContext.CreateClArray<'a>(
                    resLength,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let rightMergedValues =
                clContext.CreateClArray<'b>(
                    resLength,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let isEndOfRow =
                clContext.CreateClArray<int>(
                    resLength,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let isLeft =
                clContext.CreateClArray<int>(
                    resLength,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            // Merge
            let ndRange =
                Range1D.CreateValid(matrixLeftRowPointers.Length * workGroupSize, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            matrixLeftRowPointers.Length
                            matrixLeftValues.Length
                            matrixRightValues.Length
                            resRowSizes
                            matrixLeftRowPointers
                            matrixLeftColumns
                            matrixLeftValues
                            matrixRightRowPointers
                            matrixRightColumns
                            matrixRightValues
                            allColumns
                            leftMergedValues
                            rightMergedValues
                            isEndOfRow
                            isLeft)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

//            processor.PostAndReply(Msg.MsgNotifyMe)
//            let cols = Array.zeroCreate resRowLength
//            let isEnd = Array.zeroCreate resRowLength
//            processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(allColumns, cols, ch))
//            processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(isEndOfRow, isEnd, ch))
//            printfn "%A" cols
//            printfn "%A" isEnd

            processor.Post(Msg.CreateFreeMsg<_>(resRowSizes))

            allColumns, leftMergedValues, rightMergedValues, isEndOfRow, isLeft

    let eWiseAdd<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let merge = merge clContext workGroupSize

        let preparePositions =
            preparePositions clContext opAdd workGroupSize

        let setPositions = setPositions<'c> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'b>) ->

            let allColumns, leftMergedValues, rightMergedValues, isRowEnd, isLeft =
                merge
                    queue
                    matrixLeft.RowPointers
                    matrixLeft.Columns
                    matrixLeft.Values
                    matrixRight.RowPointers
                    matrixRight.Columns
                    matrixRight.Values

            let rawPositions, allValues =
                preparePositions queue allColumns leftMergedValues rightMergedValues isRowEnd isLeft

            queue.Post(Msg.CreateFreeMsg<_>(leftMergedValues))
            queue.Post(Msg.CreateFreeMsg<_>(rightMergedValues))

            let resultColumns, resultValues, resultLength =
                setPositions queue allColumns allValues rawPositions

            queue.Post(Msg.CreateFreeMsg<_>(isLeft))
            queue.Post(Msg.CreateFreeMsg<_>(isRowEnd))
            queue.Post(Msg.CreateFreeMsg<_>(rawPositions))
            queue.Post(Msg.CreateFreeMsg<_>(allColumns))
            queue.Post(Msg.CreateFreeMsg<_>(allValues))

            { Context = clContext
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              RowPointers = clContext.CreateClArray matrixLeft.RowCount
              Columns = resultColumns
              Values = resultValues
              }
