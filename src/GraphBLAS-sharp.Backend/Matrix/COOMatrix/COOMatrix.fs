namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module COOMatrix =
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let private setPositions<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let setPositions =
            <@ fun (ndRange: Range1D) prefixSumArrayLength (allRowsBuffer: ClArray<int>) (allColumnsBuffer: ClArray<int>) (allValuesBuffer: ClArray<'a>) (prefixSumArrayBuffer: ClArray<int>) (resultRowsBuffer: ClArray<int>) (resultColumnsBuffer: ClArray<int>) (resultValuesBuffer: ClArray<'a>) ->

                let i = ndRange.GlobalID0

                if i = prefixSumArrayLength - 1
                   || i < prefixSumArrayLength
                      && prefixSumArrayBuffer.[i]
                         <> prefixSumArrayBuffer.[i + 1] then
                    let index = prefixSumArrayBuffer.[i]

                    resultRowsBuffer.[index] <- allRowsBuffer.[i]
                    resultColumnsBuffer.[index] <- allColumnsBuffer.[i]
                    resultValuesBuffer.[index] <- allValuesBuffer.[i] @>

        let kernel =
            clContext.Compile(setPositions)

        let sum =
            GraphBLAS.FSharp.Backend.ClArray.prefixSumExcludeInplace clContext workGroupSize

        let resultLength = Array.zeroCreate 1

        fun (processor: MailboxProcessor<_>) (allRows: ClArray<int>) (allColumns: ClArray<int>) (allValues: ClArray<'a>) (positions: ClArray<int>) ->
            let prefixSumArrayLength = positions.Length

            let resultLengthGpu = clContext.CreateClCell 0

            let _, r = sum processor positions resultLengthGpu

            let resultLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            let resultRows =
                clContext.CreateClArray<int>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

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
                            allRows
                            allColumns
                            allValues
                            positions
                            resultRows
                            resultColumns
                            resultValues)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultRows, resultColumns, resultValues, resultLength

    let private preparePositions<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let preparePositions =
            <@ fun (ndRange: Range1D) length (allRowsBuffer: ClArray<int>) (allColumnsBuffer: ClArray<int>) (leftValuesBuffer: ClArray<'a>) (rightValuesBuffer: ClArray<'b>) (allValuesBuffer: ClArray<'c>) (rawPositionsBuffer: ClArray<int>) (isLeftBitmap: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if (i < length - 1
                    && allRowsBuffer.[i] = allRowsBuffer.[i + 1]
                    && allColumnsBuffer.[i] = allColumnsBuffer.[i + 1]) then
                    rawPositionsBuffer.[i] <- 0

                    match (%opAdd) (Some leftValuesBuffer.[i + 1]) (Some rightValuesBuffer.[i]) with
                    | Some v ->
                        allValuesBuffer.[i + 1] <- v
                        rawPositionsBuffer.[i + 1] <- 1
                    | None -> rawPositionsBuffer.[i + 1] <- 0
                else if (i > 0
                         && i < length
                         && (allRowsBuffer.[i] <> allRowsBuffer.[i - 1]
                             || allColumnsBuffer.[i] <> allColumnsBuffer.[i - 1]))
                        || i = 0 then
                    if isLeftBitmap.[i] = 1 then
                        match (%opAdd) (Some leftValuesBuffer.[i]) None with
                        | Some v ->
                            allValuesBuffer.[i] <- v
                            rawPositionsBuffer.[i] <- 1
                        | None -> rawPositionsBuffer.[i] <- 0
                    else
                        match (%opAdd) None (Some rightValuesBuffer.[i]) with
                        | Some v ->
                            allValuesBuffer.[i] <- v
                            rawPositionsBuffer.[i] <- 1
                        | None -> rawPositionsBuffer.[i] <- 0 @>

        let kernel =
            clContext.Compile(preparePositions)

        fun (processor: MailboxProcessor<_>) (allRows: ClArray<int>) (allColumns: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isLeft: ClArray<int>) ->
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
                            allRows
                            allColumns
                            leftValues
                            rightValues
                            allValues
                            rawPositionsGpu
                            isLeft)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
            rawPositionsGpu, allValues

    let private merge<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) workGroupSize =

        let merge =
            <@ fun (ndRange: Range1D) firstSide secondSide sumOfSides (firstRowsBuffer: ClArray<int>) (firstColumnsBuffer: ClArray<int>) (firstValuesBuffer: ClArray<'a>) (secondRowsBuffer: ClArray<int>) (secondColumnsBuffer: ClArray<int>) (secondValuesBuffer: ClArray<'b>) (allRowsBuffer: ClArray<int>) (allColumnsBuffer: ClArray<int>) (leftMergedValuesBuffer: ClArray<'a>) (rightMergedValuesBuffer: ClArray<'b>) (isLeftBitmap: ClArray<int>) ->

                let i = ndRange.GlobalID0

                let mutable beginIdxLocal = local ()
                let mutable endIdxLocal = local ()
                let localID = ndRange.LocalID0

                if localID < 2 then
                    let mutable x = localID * (workGroupSize - 1) + i - 1

                    if x >= sumOfSides then
                        x <- sumOfSides - 1

                    let diagonalNumber = x

                    let mutable leftEdge = diagonalNumber + 1 - secondSide
                    if leftEdge < 0 then leftEdge <- 0

                    let mutable rightEdge = firstSide - 1

                    if rightEdge > diagonalNumber then
                        rightEdge <- diagonalNumber

                    while leftEdge <= rightEdge do
                        let middleIdx = (leftEdge + rightEdge) / 2

                        let firstIndex: uint64 =
                            ((uint64 firstRowsBuffer.[middleIdx]) <<< 32)
                            ||| (uint64 firstColumnsBuffer.[middleIdx])

                        let secondIndex: uint64 =
                            ((uint64 secondRowsBuffer.[diagonalNumber - middleIdx])
                             <<< 32)
                            ||| (uint64 secondColumnsBuffer.[diagonalNumber - middleIdx])

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

                if endIdx = firstSide then
                    x <- secondSide - i + localID + beginIdx

                let secondLocalLength = x

                //First indices are from 0 to firstLocalLength - 1 inclusive
                //Second indices are from firstLocalLength to firstLocalLength + secondLocalLength - 1 inclusive
                let localIndices = localArray<uint64> workGroupSize

                if localID < firstLocalLength then
                    localIndices.[localID] <-
                        ((uint64 firstRowsBuffer.[beginIdx + localID])
                         <<< 32)
                        ||| (uint64 firstColumnsBuffer.[beginIdx + localID])

                if localID < secondLocalLength then
                    localIndices.[firstLocalLength + localID] <-
                        ((uint64 secondRowsBuffer.[i - beginIdx]) <<< 32)
                        ||| (uint64 secondColumnsBuffer.[i - beginIdx])

                barrierLocal ()

                if i < sumOfSides then
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

                    if not isValidX || isValidY && fstIdx < sndIdx then
                        allRowsBuffer.[i] <- int (sndIdx >>> 32)
                        allColumnsBuffer.[i] <- int sndIdx
                        rightMergedValuesBuffer.[i] <- secondValuesBuffer.[i - localID - beginIdx + boundaryY]
                        isLeftBitmap.[i] <- 0
                    else
                        allRowsBuffer.[i] <- int (fstIdx >>> 32)
                        allColumnsBuffer.[i] <- int fstIdx
                        leftMergedValuesBuffer.[i] <- firstValuesBuffer.[beginIdx + boundaryX]
                        isLeftBitmap.[i] <- 1 @>

        let kernel = clContext.Compile(merge)

        fun (processor: MailboxProcessor<_>) (matrixLeftRows: ClArray<int>) (matrixLeftColumns: ClArray<int>) (matrixLeftValues: ClArray<'a>) (matrixRightRows: ClArray<int>) (matrixRightColumns: ClArray<int>) (matrixRightValues: ClArray<'b>) ->

            let firstSide = matrixLeftValues.Length
            let secondSide = matrixRightValues.Length
            let sumOfSides = firstSide + secondSide

            let allRows =
                clContext.CreateClArray<int>(
                    sumOfSides,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let allColumns =
                clContext.CreateClArray<int>(
                    sumOfSides,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let leftMergedValues =
                clContext.CreateClArray<'a>(
                    sumOfSides,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let rightMergedValues =
                clContext.CreateClArray<'b>(
                    sumOfSides,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let isLeft =
                clContext.CreateClArray<int>(
                    sumOfSides,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let ndRange =
                Range1D.CreateValid(sumOfSides, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            firstSide
                            secondSide
                            sumOfSides
                            matrixLeftRows
                            matrixLeftColumns
                            matrixLeftValues
                            matrixRightRows
                            matrixRightColumns
                            matrixRightValues
                            allRows
                            allColumns
                            leftMergedValues
                            rightMergedValues
                            isLeft)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allRows, allColumns, leftMergedValues, rightMergedValues, isLeft

    ///<param name="clContext">.</param>
    ///<param name="opAdd">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let eWiseAdd<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let merge = merge clContext workGroupSize

        let preparePositions =
            preparePositions clContext opAdd workGroupSize

        let setPositions = setPositions<'c> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: COOMatrix<'a>) (matrixRight: COOMatrix<'b>) ->

            let allRows, allColumns, leftMergedValues, rightMergedValues, isLeft =
                merge
                    queue
                    matrixLeft.Rows
                    matrixLeft.Columns
                    matrixLeft.Values
                    matrixRight.Rows
                    matrixRight.Columns
                    matrixRight.Values

            let rawPositions, allValues =
                preparePositions queue allRows allColumns leftMergedValues rightMergedValues isLeft

            queue.Post(Msg.CreateFreeMsg<_>(leftMergedValues))
            queue.Post(Msg.CreateFreeMsg<_>(rightMergedValues))

            let resultRows, resultColumns, resultValues, resultLength =
                setPositions queue allRows allColumns allValues rawPositions

            queue.Post(Msg.CreateFreeMsg<_>(isLeft))
            queue.Post(Msg.CreateFreeMsg<_>(rawPositions))
            queue.Post(Msg.CreateFreeMsg<_>(allRows))
            queue.Post(Msg.CreateFreeMsg<_>(allColumns))
            queue.Post(Msg.CreateFreeMsg<_>(allValues))

            { Context = clContext
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              Rows = resultRows
              Columns = resultColumns
              Values = resultValues }

    let getTuples (clContext: ClContext) =

        let copy =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext

        let copyData =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext

        fun (processor: MailboxProcessor<_>) workGroupSize (matrix: COOMatrix<'a>) ->

            let resultRows = copy processor workGroupSize matrix.Rows

            let resultColumns =
                copy processor workGroupSize matrix.Columns

            let resultValues =
                copyData processor workGroupSize matrix.Values


            { Context = clContext
              RowIndices = resultRows
              ColumnIndices = resultColumns
              Values = resultValues }

    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let private compressRows (clContext: ClContext) workGroupSize =

        let calcHyperSparseRows =
            <@ fun (ndRange: Range1D) (rowsIndices: ClArray<int>) (bitmap: ClArray<int>) (positions: ClArray<int>) (nonZeroRowsIndices: ClArray<int>) (nonZeroRowsPointers: ClArray<int>) nnz ->

                let gid = ndRange.GlobalID0

                if gid < nnz && bitmap.[gid] = 1 then
                    nonZeroRowsIndices.[positions.[gid]] <- rowsIndices.[gid]
                    nonZeroRowsPointers.[positions.[gid]] <- gid + 1 @>

        let calcNnzPerRowSparse =
            <@ fun (ndRange: Range1D) (nonZeroRowsPointers: ClArray<int>) (nnzPerRowSparse: ClArray<int>) totalSum ->

                let gid = ndRange.GlobalID0

                if gid = 0 then
                    nnzPerRowSparse.[gid] <- nonZeroRowsPointers.[gid]
                elif gid < totalSum then
                    nnzPerRowSparse.[gid] <-
                        nonZeroRowsPointers.[gid]
                        - nonZeroRowsPointers.[gid - 1] @>

        let expandNnzPerRow =
            <@ fun (ndRange: Range1D) totalSum (nnzPerRowSparse: ClArray<'a>) (nonZeroRowsIndices: ClArray<int>) (expandedNnzPerRow: ClArray<'a>) ->

                let i = ndRange.GlobalID0

                if i < totalSum then
                    expandedNnzPerRow.[nonZeroRowsIndices.[i] + 1] <- nnzPerRowSparse.[i] @>

        let kernelCalcHyperSparseRows =
            clContext.Compile(calcHyperSparseRows)

        let kernelCalcNnzPerRowSparse =
            clContext.Compile(calcNnzPerRowSparse)

        let kernelExpandNnzPerRow =
            clContext.Compile(expandNnzPerRow)

        let getUniqueBitmap = ClArray.getUniqueBitmap clContext

        let posAndTotalSum =
            ClArray.prefixSumExclude clContext workGroupSize

        let getRowPointers =
            ClArray.prefixSumInclude clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (rowIndices: ClArray<int>) rowCount ->
            let bitmap =
                getUniqueBitmap processor workGroupSize rowIndices

            let positions, totalSum = posAndTotalSum processor bitmap

            let hostTotalSum = [| 0 |]

            let _ =
                processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(totalSum, hostTotalSum, ch))

            let totalSum = hostTotalSum.[0]

            let nonZeroRowsIndices =
                clContext.CreateClArray(
                    totalSum,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let nonZeroRowsPointers =
                clContext.CreateClArray(
                    totalSum,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let nnz = rowIndices.Length
            let ndRangeCHSR = Range1D.CreateValid(nnz, workGroupSize)

            let kernelCalcHyperSparseRows = kernelCalcHyperSparseRows.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernelCalcHyperSparseRows.KernelFunc
                            ndRangeCHSR
                            rowIndices
                            bitmap
                            positions
                            nonZeroRowsIndices
                            nonZeroRowsPointers
                            nnz)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernelCalcHyperSparseRows)
            processor.Post(Msg.CreateFreeMsg(bitmap))
            processor.Post(Msg.CreateFreeMsg(positions))

            let nnzPerRowSparse =
                clContext.CreateClArray(
                    totalSum,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRangeCNPRSandENPR =
                Range1D.CreateValid(totalSum, workGroupSize)

            let kernelCalcNnzPerRowSparse = kernelCalcNnzPerRowSparse.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernelCalcNnzPerRowSparse.KernelFunc
                            ndRangeCNPRSandENPR
                            nonZeroRowsPointers
                            nnzPerRowSparse
                            totalSum)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernelCalcNnzPerRowSparse)

            let expandedNnzPerRow =
                clContext.CreateClArray(
                    Array.zeroCreate rowCount,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite
                )

            let kernelExpandNnzPerRow = kernelExpandNnzPerRow.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernelExpandNnzPerRow.KernelFunc
                            ndRangeCNPRSandENPR
                            totalSum
                            nnzPerRowSparse
                            nonZeroRowsIndices
                            expandedNnzPerRow)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernelExpandNnzPerRow)

            let rowPointers, _ =
                getRowPointers processor expandedNnzPerRow

            processor.Post(Msg.CreateFreeMsg(expandedNnzPerRow))
            processor.Post(Msg.CreateFreeMsg(nnzPerRowSparse))
            processor.Post(Msg.CreateFreeMsg(nonZeroRowsIndices))
            processor.Post(Msg.CreateFreeMsg(nonZeroRowsPointers))

            rowPointers

    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSR (clContext: ClContext) workGroupSize =

        let compressRows = compressRows clContext workGroupSize

        let copy =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext

        let copyData =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext

        fun (processor: MailboxProcessor<_>) (matrix: COOMatrix<'a>) ->
            let compressedRows =
                compressRows processor matrix.Rows matrix.RowCount

            let cols =
                copy processor workGroupSize matrix.Columns

            let vals =
                copyData processor workGroupSize matrix.Values

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = compressedRows
              Columns = cols
              Values = vals }

    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSRInplace (clContext: ClContext) workGroupSize =

        let compressRows = compressRows clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: COOMatrix<'a>) ->
            let compressedRows =
                compressRows processor matrix.Rows matrix.RowCount

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = compressedRows
              Columns = matrix.Columns
              Values = matrix.Values }

    let private preparePositionsAtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        let preparePositions =
            <@ fun (ndRange: Range1D) length (allRowsBuffer: ClArray<int>) (allColumnsBuffer: ClArray<int>) (leftValuesBuffer: ClArray<'a>) (rightValuesBuffer: ClArray<'b>) (allValuesBuffer: ClArray<'c>) (rawPositionsBuffer: ClArray<int>) (isLeftBitmap: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if (i < length - 1
                    && allRowsBuffer.[i] = allRowsBuffer.[i + 1]
                    && allColumnsBuffer.[i] = allColumnsBuffer.[i + 1]) then
                    rawPositionsBuffer.[i] <- 0

                    match (%opAdd) (Both(leftValuesBuffer.[i + 1], rightValuesBuffer.[i])) with
                    | Some v ->
                        allValuesBuffer.[i + 1] <- v
                        rawPositionsBuffer.[i + 1] <- 1
                    | None -> rawPositionsBuffer.[i + 1] <- 0
                else if (i > 0
                         && i < length
                         && (allRowsBuffer.[i] <> allRowsBuffer.[i - 1]
                             || allColumnsBuffer.[i] <> allColumnsBuffer.[i - 1]))
                        || i = 0 then
                    if isLeftBitmap.[i] = 1 then
                        match (%opAdd) (Left leftValuesBuffer.[i]) with
                        | Some v ->
                            allValuesBuffer.[i] <- v
                            rawPositionsBuffer.[i] <- 1
                        | None -> rawPositionsBuffer.[i] <- 0
                    else
                        match (%opAdd) (Right rightValuesBuffer.[i]) with
                        | Some v ->
                            allValuesBuffer.[i] <- v
                            rawPositionsBuffer.[i] <- 1
                        | None -> rawPositionsBuffer.[i] <- 0 @>

        let kernel =
            clContext.Compile(preparePositions)

        fun (processor: MailboxProcessor<_>) (allRows: ClArray<int>) (allColumns: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isLeft: ClArray<int>) ->
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
                            allRows
                            allColumns
                            leftValues
                            rightValues
                            allValues
                            rawPositionsGpu
                            isLeft)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
            rawPositionsGpu, allValues

    ///<param name="clContext">.</param>
    ///<param name="opAdd">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let eWiseAddAtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        let merge = merge clContext workGroupSize

        let preparePositions =
            preparePositionsAtLeastOne clContext opAdd workGroupSize

        let setPositions = setPositions<'c> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: COOMatrix<'a>) (matrixRight: COOMatrix<'b>) ->

            let allRows, allColumns, leftMergedValues, rightMergedValues, isLeft =
                merge
                    queue
                    matrixLeft.Rows
                    matrixLeft.Columns
                    matrixLeft.Values
                    matrixRight.Rows
                    matrixRight.Columns
                    matrixRight.Values

            let rawPositions, allValues =
                preparePositions queue allRows allColumns leftMergedValues rightMergedValues isLeft

            queue.Post(Msg.CreateFreeMsg<_>(leftMergedValues))
            queue.Post(Msg.CreateFreeMsg<_>(rightMergedValues))

            let resultRows, resultColumns, resultValues, resultLength =
                setPositions queue allRows allColumns allValues rawPositions

            queue.Post(Msg.CreateFreeMsg<_>(isLeft))
            queue.Post(Msg.CreateFreeMsg<_>(rawPositions))
            queue.Post(Msg.CreateFreeMsg<_>(allRows))
            queue.Post(Msg.CreateFreeMsg<_>(allColumns))
            queue.Post(Msg.CreateFreeMsg<_>(allValues))

            { Context = clContext
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              Rows = resultRows
              Columns = resultColumns
              Values = resultValues }
