namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module EWiseAddBatched =
    let merge<'a when 'a : struct> (clContext: ClContext) workGroupSize =
        let merge =
            <@
                fun (ndRange: Range1D)
                    firstSide
                    secondSide
                    sumOfSides
                    (firstRowsBuffer: ClArray<int>)
                    (firstColumnsBuffer: ClArray<int>)
                    (firstValuesBuffer: ClArray<'a>)
                    (secondRowsBuffer: ClArray<int>)
                    (secondColumnsBuffer: ClArray<int>)
                    (secondValuesBuffer: ClArray<'a>)
                    (allRowsBuffer: ClArray<int>)
                    (allColumnsBuffer: ClArray<int>)
                    (allValuesBuffer: ClArray<'a>) ->

                    let i = ndRange.GlobalID0

                    let mutable beginIdxLocal = local ()
                    let mutable endIdxLocal = local ()
                    let localID = ndRange.LocalID0

                    if localID < 2 then
                        let mutable x = localID * (workGroupSize - 1) + i - 1

                        if x >= sumOfSides then x <- sumOfSides - 1

                        let diagonalNumber = x

                        let mutable leftEdge = diagonalNumber + 1 - secondSide
                        if leftEdge < 0 then leftEdge <- 0

                        let mutable rightEdge = firstSide - 1

                        if rightEdge > diagonalNumber then rightEdge <- diagonalNumber

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2

                            let firstIndex: uint64 =
                                ((uint64 firstRowsBuffer.[middleIdx]) <<< 32)
                                ||| (uint64 firstColumnsBuffer.[middleIdx])

                            let secondIndex: uint64 =
                                ((uint64 secondRowsBuffer.[diagonalNumber - middleIdx]) <<< 32)
                                ||| (uint64 secondColumnsBuffer.[diagonalNumber - middleIdx])

                            if firstIndex < secondIndex then
                                leftEdge <- middleIdx + 1
                            else
                                rightEdge <- middleIdx - 1

                        // Here localID equals either 0 or 1
                        if localID = 0 then beginIdxLocal <- leftEdge else endIdxLocal <- leftEdge

                    barrierLocal ()

                    let beginIdx = beginIdxLocal
                    let endIdx = endIdxLocal
                    let firstLocalLength = endIdx - beginIdx
                    let mutable x = workGroupSize - firstLocalLength

                    if endIdx = firstSide then x <- secondSide - i + localID + beginIdx

                    let secondLocalLength = x

                    //First indices are from 0 to firstLocalLength - 1 inclusive
                    //Second indices are from firstLocalLength to firstLocalLength + secondLocalLength - 1 inclusive
                    let localIndices = localArray<uint64> workGroupSize

                    if localID < firstLocalLength then
                        localIndices.[localID] <-
                            ((uint64 firstRowsBuffer.[beginIdx + localID]) <<< 32)
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

                        if rightEdge > localID then rightEdge <- localID

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstIndex = localIndices.[middleIdx]

                            let secondIndex = localIndices.[firstLocalLength + localID - middleIdx]

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

                        if isValidX then fstIdx <- localIndices.[boundaryX]

                        let mutable sndIdx = 0UL

                        if isValidY then sndIdx <- localIndices.[firstLocalLength + boundaryY]

                        if not isValidX || isValidY && fstIdx < sndIdx then
                            allRowsBuffer.[i] <- int (sndIdx >>> 32)
                            allColumnsBuffer.[i] <- int sndIdx
                            allValuesBuffer.[i] <- secondValuesBuffer.[i - localID - beginIdx + boundaryY]
                        else
                            allRowsBuffer.[i] <- int (fstIdx >>> 32)
                            allColumnsBuffer.[i] <- int fstIdx
                            allValuesBuffer.[i] <- firstValuesBuffer.[beginIdx + boundaryX]
            @>

        let kernel = clContext.Compile(merge)

        fun (processor: MailboxProcessor<_>)
            (matrixLeftRows: ClArray<int>)
            (matrixLeftColumns: ClArray<int>)
            (matrixLeftValues: ClArray<'a>)
            (matrixRightRows: ClArray<int>)
            (matrixRightColumns: ClArray<int>)
            (matrixRightValues: ClArray<'a>) ->

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

            let allValues =
                clContext.CreateClArray<'a>(
                    sumOfSides,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )


            let ndRange = Range1D.CreateValid(sumOfSides, workGroupSize)
            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () ->
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
                        allValues
                )
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allRows, allColumns, allValues

    let combineIndices (clContext: ClContext) workGroupSize =
        let command =
            <@
                fun (range: Range1D)
                    (rowsBuffer: ClArray<int>)
                    (colsBuffer: ClArray<int>)
                    (indicesBufferLength: int)
                    (colsCount: int)
                    (newIndecies: ClArray<int>) ->

                    let gid = range.GlobalID0

                    if gid < indicesBufferLength then
                        newIndecies.[gid] <- rowsBuffer.[gid] * colsCount + colsBuffer.[gid]
            @>

        let program = clContext.Compile(command)

        fun (processor: MailboxProcessor<_>)
            (rowsBuffer: ClArray<int>)
            (colsBuffer: ClArray<int>)
            (colsCount: int) ->

            let length = rowsBuffer.Length
            let newIndecies = clContext.CreateClArray<int>(length)

            let ndRange = Range1D.CreateValid(length, workGroupSize)
            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () ->
                    kernel.KernelFunc
                        ndRange
                        rowsBuffer
                        colsBuffer
                        length
                        colsCount
                        newIndecies
                )
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            newIndecies

    let markUniqueIndices<'a when 'a: struct> (clContext: ClContext) workGroupSize =
        let command =
            <@
                fun (ndRange: Range1D)
                    (indicesBuffer: ClArray<int>)
                    (indicesBufferLength: int)
                    (isUniqueBitmap: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < indicesBufferLength then
                        isUniqueBitmap.[i] <- 1

                    if i < indicesBufferLength - 1 && indicesBuffer.[i] = indicesBuffer.[i + 1] then
                        isUniqueBitmap.[i] <- 0
            @>

        let program = clContext.Compile(command)

        fun (processor: MailboxProcessor<_>) (indices: ClArray<int>) ->
            let length = indices.Length

            let isUniqueBitmap =
                clContext.CreateClArray<int>(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let ndRange = Range1D.CreateValid(length, workGroupSize)
            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () ->
                    kernel.KernelFunc
                        ndRange
                        indices
                        length
                        isUniqueBitmap
                )
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            isUniqueBitmap

    let calculateValues (clContext: ClContext) (op: Expr<'a option -> 'a option -> 'a option>) workGroupSize  =
        let command =
            <@
                fun (range: Range1D)
                    (isUniqueBitmap: ClArray<int>)
                    (isUniqueBitmapLength: int)
                    (allValues: ClArray<'a>)
                    (newValues: ClArray<'a>)
                    (newBitmap: ClArray<int>) ->

                    let gid = range.GlobalID0

                    if
                        gid > 0 && gid < isUniqueBitmapLength &&
                        isUniqueBitmap.[gid] = 1 && isUniqueBitmap.[gid - 1] = 0
                    then
                        let mutable acc = Some allValues.[gid]
                        let mutable i = 1
                        while gid - i >= 0 && isUniqueBitmap.[gid - i] = 0 do
                            match (%op) acc (Some allValues.[gid - i]) with
                            | Some v -> acc <- Some v
                            | None -> acc <- None

                            i <- i + 1

                        match acc with
                        | Some v -> newValues.[gid] <- v
                        | None -> newBitmap.[gid] <- 0

                    elif gid < isUniqueBitmapLength && isUniqueBitmap.[gid] = 1 then
                        match Some allValues.[gid] with
                        | Some v -> newValues.[gid] <- allValues.[gid]
                        | None -> newBitmap.[gid] <- 0
            @>

        let program = clContext.Compile(command)

        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (isUniqueBitmap: ClArray<int>)
            (allValues: ClArray<'a>) ->

            let length = isUniqueBitmap.Length
            let newValues = clContext.CreateClArray<'a>(length)
            let newBitmap = copy processor isUniqueBitmap

            let ndRange = Range1D.CreateValid(length, workGroupSize)
            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () ->
                    kernel.KernelFunc
                        ndRange
                        isUniqueBitmap
                        length
                        allValues
                        newValues
                        newBitmap
                )
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            newValues, newBitmap

    let private setPositions<'a when 'a: struct> (clContext: ClContext) workGroupSize =
        let setPositions =
            <@
                fun (ndRange: Range1D)
                    (indicesBuffer: ClArray<int>)
                    (indicesBufferLength: int)
                    (newBitmap: ClArray<int>)
                    (valuesBuffer: ClArray<'a>)
                    (positionsBuffer: ClArray<int>)
                    (colCount: int)
                    (resultRowsBuffer: ClArray<int>)
                    (resultColumnsBuffer: ClArray<int>)
                    (resultValuesBuffer: ClArray<'a>) ->

                    let i = ndRange.GlobalID0

                    if
                       i = indicesBufferLength - 1 ||
                       i < indicesBufferLength && newBitmap.[i] = 1
                    then
                        let index = positionsBuffer.[i]

                        resultRowsBuffer.[index] <- indicesBuffer.[i] / colCount
                        resultColumnsBuffer.[index] <- indicesBuffer.[i] % colCount
                        resultValuesBuffer.[index] <- valuesBuffer.[i]
            @>

        let kernel = clContext.Compile(setPositions)

        fun (processor: MailboxProcessor<_>)
            (indices: ClArray<int>)
            newBitmap
            (values: ClArray<'a>)
            (positions: int clarray)
            colCount
            (resultLength: int) ->

            let prefixSumArrayLength = positions.Length

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

            let ndRange = Range1D.CreateValid(positions.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () ->
                    kernel.KernelFunc
                        ndRange
                        indices
                        prefixSumArrayLength
                        newBitmap
                        values
                        positions
                        colCount
                        resultRows
                        resultColumns
                        resultValues
                )
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultRows, resultColumns, resultValues

    let eWiseAddBatched<'a when 'a: struct>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'a option -> 'a option>)
        workGroupSize
        : MailboxProcessor<Msg> -> ClCooMatrix<'a>[] -> ClCooMatrix<'a> =

        let merge = merge clContext workGroupSize
        let combineIndices = combineIndices clContext workGroupSize
        let markUniqueIndices = markUniqueIndices clContext workGroupSize
        let calculateValues = calculateValues clContext opAdd workGroupSize
        let scanExclude = ClArray.prefixSumExclude clContext workGroupSize
        let setPositions = setPositions clContext workGroupSize
        let getTuples = COOMatrix.getTuples clContext workGroupSize
        let copy = ClArray.copy clContext workGroupSize
        let copyData = ClArray.copy clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrices: ClCooMatrix<'a>[])  ->
            let mutable mergedRows, mergedColumns, mergedValues =
                copy queue matrices.[0].Rows,
                copy queue matrices.[0].Columns,
                copyData queue matrices.[0].Values

            for i in 1 .. matrices.Length - 1 do
                let tmpRows, tmpCols, tmpValues =
                    merge queue mergedRows mergedColumns mergedValues matrices.[i].Rows matrices.[i].Columns matrices.[i].Values

                queue.Post(Msg.CreateFreeMsg<_>(mergedRows))
                queue.Post(Msg.CreateFreeMsg<_>(mergedColumns))
                queue.Post(Msg.CreateFreeMsg<_>(mergedValues))
                queue.PostAndReply(Msg.MsgNotifyMe)

                mergedRows <- tmpRows
                mergedColumns <- tmpCols
                mergedValues <- tmpValues

            let combinedIndices = combineIndices queue mergedRows mergedColumns matrices.[0].ColumnCount
            queue.Post(Msg.CreateFreeMsg<_>(mergedRows))
            queue.Post(Msg.CreateFreeMsg<_>(mergedColumns))

            let isUniqueBitmap = markUniqueIndices queue combinedIndices

            let newValues, newBitmap = calculateValues queue isUniqueBitmap mergedValues
            queue.Post(Msg.CreateFreeMsg<_>(mergedValues))
            queue.Post(Msg.CreateFreeMsg<_>(isUniqueBitmap))

            let positions, sum = scanExclude queue newBitmap
            let resultLength = Array.zeroCreate 1
            let resultLength =
                let res = queue.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(sum, resultLength, ch))
                queue.Post(Msg.CreateFreeMsg<_>(sum))
                res.[0]

            let rows, columns, values =
                setPositions queue combinedIndices newBitmap newValues positions  matrices.[0].ColumnCount resultLength

            {
                Context = clContext
                RowCount = matrices.[0].RowCount
                ColumnCount = matrices.[0].ColumnCount
                Rows = rows
                Columns = columns
                Values = values
            }
