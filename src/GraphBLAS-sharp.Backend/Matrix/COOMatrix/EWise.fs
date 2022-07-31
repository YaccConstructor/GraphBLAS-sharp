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

                let row = ndRange.GlobalID0

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

                    let i = ndRange.GlobalID0

                    if i < rowSize
                    then
                        destinationCols.[dstOffset + i] <- sourceCols.[srcOffset + i]
                        destinationValues.[dstOffset + i] <- sourceValues.[srcOffset + i]
                        isLeftBitmap.[dstOffset + i] <- 1
                        isEndOfRowBitmap.[dstOffset + i] <- if i = rowSize - 1 then 1 else 0 @>

        let copyWithOffsetRight =
            <@ fun (ndRange: Range1D) rowSize srcOffset dstOffset (sourceCols: ClArray<int>) (sourceValues: ClArray<'a>) (destinationCols: ClArray<int>) (destinationValues: ClArray<'a>) (isEndOfRowBitmap: ClArray<int>) (isLeftBitmap: ClArray<int>) ->

                    let i = ndRange.GlobalID0

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

                let i = ndRange.GlobalID0

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

                let i = ndRange.GlobalID0

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

                let i = ndRange.GlobalID0

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
                (isLeftBitmap: ClArray<int>)
                (leftLocal: ClArray<int>)
                (rightLocal: ClArray<int>) ->

                    let globalID = ndRange.GlobalID0
                    let localID = ndRange.LocalID0
                    let MaxVal = Int32.MaxValue

                    let row = globalID / workGroupSize

                    let firstOffset = firstRowPointers.[row]
                    let secondOffset = secondRowPointers.[row]
                    let resOffset = firstOffset + secondOffset

                    let firstRowEnd = if row = rows - 1 then firstNNZ else firstRowPointers.[row + 1]
                    let secondRowEnd = if row = rows - 1 then secondNNZ else secondRowPointers.[row + 1]

                    let firstRowLength = firstRowEnd - firstOffset
                    let secondRowLength = secondRowEnd - secondOffset
                    let resRowLength = firstRowLength + secondRowLength

                    let workBlockCount = (resRowLength + workGroupSize - 1) / workGroupSize

                    //Offsets of a sliding window, computing with maxFirstIndex and maxSecondIndex on each iteration
                    let mutable firstLocalOffset = 0
                    let mutable secondLocalOffset = 0
                    let mutable maxFirstIndex = local ()
                    let mutable maxSecondIndex = local ()
                    let mutable dir = true

                    //Local arrays for column indices
                    let firstRowLocal = localArray<int> 34
                    let secondRowLocal = localArray<int> 34
                    //let resRowLocal = localArray<int> (workGroupSize + 1)
                    //if localID = 0 then resRowLocal.[workGroupSize] <- 0

                    //Cycle on each work block for one row
                    for block in 0 .. workBlockCount - 1 do

                        let mutable maxFirstIndexPerThread = 0
                        let mutable maxSecondIndexPerThread = 0

                        let firstBufferSize = min (firstRowLength - firstLocalOffset) workGroupSize
                        let secondBufferSize = min (secondRowLength - secondLocalOffset) workGroupSize

                        if localID = 0 then
                            maxFirstIndex <- 0
                            maxSecondIndex <- 0

                        //Filling local arrays for current window. First element is always MaxVal
                        for j in localID .. workGroupSize .. workGroupSize + 1 do
                            if j > 0 && j - 1 < firstBufferSize then
                                firstRowLocal.[j] <- firstColumns.[firstOffset + j - 1 + firstLocalOffset]
                                leftLocal.[j] <- firstColumns.[firstOffset + j - 1 + firstLocalOffset] ////
                            else
                                firstRowLocal.[j] <- MaxVal
                                leftLocal.[j] <- MaxVal ////
                            if j > 0 && j - 1 < secondBufferSize then
                                secondRowLocal.[j] <- secondColumns.[secondOffset + j - 1 + secondLocalOffset]
                                rightLocal.[j] <- secondColumns.[secondOffset + j - 1 + secondLocalOffset] ////
                            else
                                secondRowLocal.[j] <- MaxVal
                                rightLocal.[j] <- MaxVal ////

                        barrierFull ()

                        let workSize = min (firstBufferSize + secondBufferSize) workGroupSize

                        let mutable res = MaxVal

                        let i = if dir then localID else workGroupSize - 1 - localID

                        //Binary search for intersection on diagonal
                        //X axis points from left to right and corresponds to the first array
                        //Y axis points from top to bottom and corresponds to the second array
                        //Second array is prior to the first when elements are equal
                        if i < workSize then
                            let x = 0
                            let y = i + 2

                            let mutable l = 0
                            let mutable r = i + 2

                            while (r - l > 1) do
                                let mid = (r - l) / 2
                                let ans = secondRowLocal.[y - l - mid] > firstRowLocal.[x + l + mid]
                                if ans then
                                    l <- l + mid
                                else
                                    r <- r - mid

                            let resX = x + l
                            let resY = y - l

                            if resY = 1 || resX = 0 then
                                if resY = 1 then
                                    res <- firstRowLocal.[resX]
                                    leftMergedValues.[resOffset + i] <- firstValues.[firstOffset + firstLocalOffset + resX - 1] // +1???
                                    isLeftBitmap.[resOffset + i] <- 1
                                    maxFirstIndexPerThread <- max maxFirstIndexPerThread resX
                                else
                                    res <- secondRowLocal.[resY - 1]
                                    rightMergedValues.[resOffset + i] <- secondValues.[secondOffset + secondLocalOffset + resY - 1 - 1] //???
                                    isLeftBitmap.[resOffset + i] <- 0
                                    maxSecondIndexPerThread <- max maxSecondIndexPerThread (resY - 1)
                            else
                                if secondRowLocal[resY - 1] > firstRowLocal[resX] then
                                    res <- secondRowLocal.[resY - 1]
                                    rightMergedValues.[resOffset + i] <- secondValues.[secondOffset + secondLocalOffset + resY - 1 - 1] //???
                                    isLeftBitmap.[resOffset + i] <- 0
                                    maxSecondIndexPerThread <- max maxSecondIndexPerThread (resY - 1)
                                else
                                    res <- firstRowLocal.[resX]
                                    leftMergedValues.[resOffset + i] <- firstValues.[firstOffset + firstLocalOffset + resX - 1] // +1???
                                    isLeftBitmap.[resOffset + i] <- 1
                                    maxFirstIndexPerThread <- max maxFirstIndexPerThread resX

                            allColumns.[resOffset + i] <- res

                        //Moving the window of search
                        atomic (max) maxFirstIndex maxFirstIndexPerThread |> ignore
                        //atomic (max) maxSecondIndex maxSecondIndexPerThread |> ignore

                        barrierFull ()

                        dir <- not dir

                        firstLocalOffset <- firstLocalOffset + maxFirstIndex
                        secondLocalOffset <- secondLocalOffset + maxSecondIndex

                        barrierLocal () @>

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

            let leftLocal =
                clContext.CreateClArray<int>(
                    workGroupSize + 2,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let rightLocal =
                clContext.CreateClArray<int>(
                    workGroupSize + 2,
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
                            isLeft
                            leftLocal
                            rightLocal)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            processor.PostAndReply(Msg.MsgNotifyMe)
            let resValsLeftCPU = Array.zeroCreate leftMergedValues.Length
            let resValsRightCPU = Array.zeroCreate rightMergedValues.Length
            let bitmap = Array.zeroCreate isLeft.Length
            processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(leftMergedValues, resValsLeftCPU, ch))
            processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(rightMergedValues, resValsRightCPU, ch))
            processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(isLeft, bitmap, ch))
            printfn "%A" resValsLeftCPU
            printfn "%A" resValsRightCPU
            printfn "%A" bitmap

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
