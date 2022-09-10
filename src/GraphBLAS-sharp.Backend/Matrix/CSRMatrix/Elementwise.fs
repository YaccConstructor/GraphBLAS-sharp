namespace GraphBLAS.FSharp.Backend

open System
open System.Diagnostics.CodeAnalysis
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module Elementwise =
    let private expandCompressedRowPointers (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) nonZeroRows nnz numberOfRows (compressedRowPointers: ClArray<int>) (compressedRows: ClArray<int>) (rowPointers: ClArray<int>) ->

                let i = ndRange.GlobalID0

                //Init with zeroes
                if i < numberOfRows then rowPointers.[i] <- 0

                if i < nonZeroRows then
                    rowPointers.[compressedRows.[i]] <-
                        if i = nonZeroRows - 1 then nnz - compressedRowPointers.[i] else compressedRowPointers.[i + 1] - compressedRowPointers.[i] @>

        let sum = ClArray.prefixSumExcludeInplace clContext workGroupSize

        let kernel = clContext.Compile(kernel)

        fun (processor: MailboxProcessor<_>) (numberOfRows: int) (nnz: int) (compressedRowPointers: ClArray<int>) (compressedRows: ClArray<int>) ->

            let rowPointers =
                    clContext.CreateClArray<int>(
                        numberOfRows,
                        hostAccessMode = HostAccessMode.NotAccessible,
                        deviceAccessMode = DeviceAccessMode.ReadWrite,
                        allocationMode = AllocationMode.Default
                    )

            let ndRange =
                    Range1D.CreateValid(numberOfRows, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            compressedRowPointers.Length
                            nnz
                            numberOfRows
                            compressedRowPointers
                            compressedRows
                            rowPointers)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            processor.PostAndReply(Msg.MsgNotifyMe)

            let sumCell = clContext.CreateClCell 0
            sum processor rowPointers sumCell |> ignore
            //processor.Post(Msg.CreateFreeMsg<_>(sumCell))

            rowPointers

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
                elif i = 0
                     || (i < length
                     && (allColumns.[i] <> allColumns.[i - 1]
                        || isEndOfRowBitmap.[i - 1] = 1)
                     )
                then
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

            let rawPositions =
                clContext.CreateClArray<int>(
                    length,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let allValues =
                clContext.CreateClArray<'c>(
                    length,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
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
                            rawPositions
                            isEndOfRow
                            isLeft)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
            rawPositions, allValues

    let private preparePositionsAtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
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

                    match (%opAdd) (Both(leftValues.[i + 1], rightValues.[i])) with
                    | Some v ->
                        allValues.[i + 1] <- v
                        rawPositions.[i + 1] <- 1
                    | None -> rawPositions.[i + 1] <- 0
                elif i = 0
                     || (i < length
                     && allColumns.[i] <> allColumns.[i - 1]
                     || isEndOfRowBitmap.[i - 1] = 1)
                then
                    if isLeftBitmap.[i] = 1 then
                        match (%opAdd) (Left leftValues.[i]) with
                        | Some v ->
                            allValues.[i] <- v
                            rawPositions.[i] <- 1
                        | None -> rawPositions.[i] <- 0
                    else
                        match (%opAdd) (Right rightValues.[i]) with
                        | Some v ->
                            allValues.[i] <- v
                            rawPositions.[i] <- 1
                        | None -> rawPositions.[i] <- 0 @>

        let kernel = clContext.Compile(preparePositions)

        fun (processor: MailboxProcessor<_>) (allColumns: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isEndOfRow: ClArray<int>) (isLeft: ClArray<int>) ->
            let length = leftValues.Length

            let ndRange =
                Range1D.CreateValid(length, workGroupSize)

            let rawPositions =
                clContext.CreateClArray<int>(
                    length,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let allValues =
                clContext.CreateClArray<'c>(
                    length,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
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
                            rawPositions
                            isEndOfRow
                            isLeft)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
            rawPositions, allValues

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

        fun (processor: MailboxProcessor<_>) (allColumns: ClArray<int>) (allValues: ClArray<'a>) (positions: ClArray<int>) ->

            let resultLength = Array.zeroCreate 1
            let prefixSumArrayLength = positions.Length

            let resultLengthGpu = clContext.CreateClCell 0

            let _, r = sum processor positions resultLengthGpu

            processor.PostAndReply(Msg.MsgNotifyMe)

            processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))
            let resultLength = resultLength.[0]
            //processor.Post(Msg.CreateFreeMsg<_>(r))

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

            resultColumns, resultValues, positions, resultLength

    let private getCompressedRowPointers (clContext: ClContext) workGroupSize =

        let getCompressedRowPointers =
            <@ fun (ndRange: Range1D) arrayLength (allRows: ClArray<int>) (positions: ClArray<int>) (isEndOfRowBitmap: ClArray<int>) (rowPointers: ClArray<int>) (compressedRows: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i > 0
                    && i < arrayLength
                    && isEndOfRowBitmap.[i]
                        <> isEndOfRowBitmap.[i - 1] then
                    let row = isEndOfRowBitmap.[i]

                    rowPointers.[row] <- positions.[i]
                    compressedRows.[row] <- allRows.[i]
                elif i = 0 then
                    rowPointers.[0] <- 0
                    compressedRows.[0] <- allRows.[0] @>

        let kernel = clContext.Compile(getCompressedRowPointers)
        let sum = ClArray.prefixSumExcludeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (allRows: ClArray<int>) (positions: ClArray<int>) (isRowEnd: ClArray<int>) ->

            let nonZeroRows = Array.zeroCreate 1

            let rowEndSum = clContext.CreateClCell 0

            let _, rowEndSum = sum processor isRowEnd rowEndSum

            processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(rowEndSum, nonZeroRows, ch))
            let nonZeroRows = nonZeroRows.[0]
            //processor.Post(Msg.CreateFreeMsg<_>(rowEndSum))

            let compressedRowPointers =
                clContext.CreateClArray<int>(
                    nonZeroRows,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let compressedRows =
                clContext.CreateClArray<int>(
                    nonZeroRows,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange =
                Range1D.CreateValid(allRows.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            allRows.Length
                            allRows
                            positions
                            isRowEnd
                            compressedRowPointers
                            compressedRows)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            compressedRowPointers, compressedRows, nonZeroRows

    let private merge<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) workGroupSize =
        let localArraySize = workGroupSize + 2
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
                (allRows: ClArray<int>)
                (allColumns: ClArray<int>)
                (leftMergedValues: ClArray<'a>)
                (rightMergedValues: ClArray<'b>)
                (isEndOfRowBitmap: ClArray<int>)
                (isLeftBitmap: ClArray<int>) ->

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

                    //Offsets of a sliding window, computed with maxFirstIndex and maxSecondIndex on each iteration
                    let mutable firstLocalOffset = 0
                    let mutable secondLocalOffset = 0
                    let mutable maxFirstIndex = local ()
                    let mutable maxSecondIndex = local ()
                    let mutable dir = true

                    //Local arrays for column indices
                    let firstRowLocal = localArray<int> localArraySize
                    let secondRowLocal = localArray<int> localArraySize

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
                            else
                                firstRowLocal.[j] <- MaxVal
                            if j > 0 && j - 1 < secondBufferSize then
                                secondRowLocal.[j] <- secondColumns.[secondOffset + j - 1 + secondLocalOffset]
                            else
                                secondRowLocal.[j] <- MaxVal

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

                            let outputIndex = resOffset + firstLocalOffset + secondLocalOffset + i

                            if resY = 1 || resX = 0 then
                                if resY = 1 then
                                    res <- firstRowLocal.[resX]
                                    leftMergedValues.[outputIndex] <- firstValues.[firstOffset + firstLocalOffset + resX - 1]
                                    isLeftBitmap.[outputIndex] <- 1
                                    maxFirstIndexPerThread <- max maxFirstIndexPerThread resX
                                else
                                    res <- secondRowLocal.[resY - 1]
                                    rightMergedValues.[outputIndex] <- secondValues.[secondOffset + secondLocalOffset + resY - 1 - 1]
                                    isLeftBitmap.[outputIndex] <- 0
                                    maxSecondIndexPerThread <- max maxSecondIndexPerThread (resY - 1)
                            else
                                if secondRowLocal.[resY - 1] > firstRowLocal.[resX] then
                                    res <- secondRowLocal.[resY - 1]
                                    rightMergedValues.[outputIndex] <- secondValues.[secondOffset + secondLocalOffset + resY - 1 - 1]
                                    isLeftBitmap.[outputIndex] <- 0
                                    maxSecondIndexPerThread <- max maxSecondIndexPerThread (resY - 1)
                                else
                                    res <- firstRowLocal.[resX]
                                    leftMergedValues.[outputIndex] <- firstValues.[firstOffset + firstLocalOffset + resX - 1]
                                    isLeftBitmap.[outputIndex] <- 1
                                    maxFirstIndexPerThread <- max maxFirstIndexPerThread resX

                            allRows.[outputIndex] <- row
                            allColumns.[outputIndex] <- res
                            isEndOfRowBitmap.[outputIndex] <- 0

                        //Moving the window of search
                        if block < workBlockCount - 1 then
                            atomic (max) maxFirstIndex maxFirstIndexPerThread |> ignore
                            atomic (max) maxSecondIndex maxSecondIndexPerThread |> ignore

                            barrierFull ()

                            dir <- not dir

                            firstLocalOffset <- firstLocalOffset + maxFirstIndex
                            secondLocalOffset <- secondLocalOffset + maxSecondIndex

                            barrierLocal ()
                        else if i = workSize - 1 then isEndOfRowBitmap.[resOffset + firstLocalOffset + secondLocalOffset + i] <- 1 @>

        let kernel = clContext.Compile(merge)

        fun (processor: MailboxProcessor<_>) (matrixLeftRowPointers: ClArray<int>) (matrixLeftColumns: ClArray<int>) (matrixLeftValues: ClArray<'a>) (matrixRightRowPointers: ClArray<int>) (matrixRightColumns: ClArray<int>) (matrixRightValues: ClArray<'b>) ->

            let firstLength = matrixLeftValues.Length
            let secondLength = matrixRightValues.Length
            let resLength = firstLength + secondLength

            let allRows =
                clContext.CreateClArray<int>(
                    resLength,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

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
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let rightMergedValues =
                clContext.CreateClArray<'b>(
                    resLength,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let isEndOfRow =
                clContext.CreateClArray<int>(
                    resLength,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let isLeft =
                clContext.CreateClArray<int>(
                    resLength,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

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
                            allRows
                            allColumns
                            leftMergedValues
                            rightMergedValues
                            isEndOfRow
                            isLeft)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allRows, allColumns, leftMergedValues, rightMergedValues, isEndOfRow, isLeft

    let elementwise<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let merge = merge clContext workGroupSize

        let preparePositions =
            preparePositions clContext opAdd Utils.defaultWorkGroupSize

        let setPositions = setPositions<'c> clContext Utils.defaultWorkGroupSize

        let getCompressedRowPointers = getCompressedRowPointers clContext Utils.defaultWorkGroupSize

        let expandCompressedRowPointers = expandCompressedRowPointers clContext Utils.defaultWorkGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'b>) ->

            let allRows, allColumns, leftMergedValues, rightMergedValues, isRowEnd, isLeft =
                merge
                    queue
                    matrixLeft.RowPointers
                    matrixLeft.Columns
                    matrixLeft.Values
                    matrixRight.RowPointers
                    matrixRight.Columns
                    matrixRight.Values

            queue.PostAndReply(Msg.MsgNotifyMe)

            let positions, allValues =
                preparePositions queue allColumns leftMergedValues rightMergedValues isRowEnd isLeft

            queue.PostAndReply(Msg.MsgNotifyMe)

            queue.Post(Msg.CreateFreeMsg<_>(leftMergedValues))
            queue.Post(Msg.CreateFreeMsg<_>(rightMergedValues))

            let resultColumns, resultValues, positions, positionsSum =
                setPositions queue allColumns allValues positions

            queue.PostAndReply(Msg.MsgNotifyMe)

            //Getting DCSR row pointers
            let compressedRowPointers, compressedRows, nonZeroRowsSum =
                getCompressedRowPointers queue allRows positions isRowEnd

            queue.PostAndReply(Msg.MsgNotifyMe)

            //Converting DCSR to CSR
            let rowPointers = expandCompressedRowPointers queue matrixLeft.RowCount resultValues.Length compressedRowPointers compressedRows

            queue.PostAndReply(Msg.MsgNotifyMe)

            queue.Post(Msg.CreateFreeMsg<_>(compressedRowPointers))
            queue.Post(Msg.CreateFreeMsg<_>(compressedRows))
            queue.Post(Msg.CreateFreeMsg<_>(allRows))
            queue.Post(Msg.CreateFreeMsg<_>(isLeft))
            queue.Post(Msg.CreateFreeMsg<_>(isRowEnd))
            queue.Post(Msg.CreateFreeMsg<_>(positions))
            queue.Post(Msg.CreateFreeMsg<_>(allColumns))
            queue.Post(Msg.CreateFreeMsg<_>(allValues))

            { Context = clContext
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              RowPointers = rowPointers
              Columns = resultColumns
              Values = resultValues
              }

    let elementwiseAtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        let merge = merge clContext workGroupSize

        let preparePositions =
            preparePositionsAtLeastOne clContext opAdd Utils.defaultWorkGroupSize

        let setPositions = setPositions<'c> clContext Utils.defaultWorkGroupSize

        let getCompressedRowPointers = getCompressedRowPointers clContext Utils.defaultWorkGroupSize

        let expandCompressedRowPointers = expandCompressedRowPointers clContext Utils.defaultWorkGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'b>) ->

            let allRows, allColumns, leftMergedValues, rightMergedValues, isRowEnd, isLeft =
                merge
                    queue
                    matrixLeft.RowPointers
                    matrixLeft.Columns
                    matrixLeft.Values
                    matrixRight.RowPointers
                    matrixRight.Columns
                    matrixRight.Values

            queue.PostAndReply(Msg.MsgNotifyMe)

            let positions, allValues =
                preparePositions queue allColumns leftMergedValues rightMergedValues isRowEnd isLeft

            queue.PostAndReply(Msg.MsgNotifyMe)

            queue.Post(Msg.CreateFreeMsg<_>(leftMergedValues))
            queue.Post(Msg.CreateFreeMsg<_>(rightMergedValues))

            let resultColumns, resultValues, positions, positionsSum =
                setPositions queue allColumns allValues positions

            queue.PostAndReply(Msg.MsgNotifyMe)

            let compressedRowPointers, compressedRows, nonZeroRowsSum =
                getCompressedRowPointers queue allRows positions isRowEnd

            queue.PostAndReply(Msg.MsgNotifyMe)

            let rowPointers = expandCompressedRowPointers queue matrixLeft.RowCount resultValues.Length compressedRowPointers compressedRows

            queue.PostAndReply(Msg.MsgNotifyMe)

            queue.Post(Msg.CreateFreeMsg<_>(compressedRowPointers))
            queue.Post(Msg.CreateFreeMsg<_>(compressedRows))
            queue.Post(Msg.CreateFreeMsg<_>(allRows))
            queue.Post(Msg.CreateFreeMsg<_>(isLeft))
            queue.Post(Msg.CreateFreeMsg<_>(isRowEnd))
            queue.Post(Msg.CreateFreeMsg<_>(positions))
            queue.Post(Msg.CreateFreeMsg<_>(allColumns))
            queue.Post(Msg.CreateFreeMsg<_>(allValues))

            { Context = clContext
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              RowPointers = rowPointers
              Columns = resultColumns
              Values = resultValues
              }
