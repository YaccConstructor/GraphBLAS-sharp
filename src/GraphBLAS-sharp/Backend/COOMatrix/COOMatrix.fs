namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common

type TupleMatrix<'elem when 'elem: struct> =
    { RowIndices: ClArray<int>
      ColumnIndices: ClArray<int>
      Values: ClArray<'elem> }

type COOMatrix<'elem when 'elem: struct> =
    { RowCount: int
      ColumnCount: int
      Rows: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }

module COOMatrix =
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

        let kernel = clContext.CreateClKernel(setPositions)

        let sum =
            GraphBLAS.FSharp.Backend.ClArray.prefixSumExcludeInplace clContext

        let resultLength = Array.zeroCreate 1

        fun (processor: MailboxProcessor<_>) (allRows: ClArray<int>) (allColumns: ClArray<int>) (allValues: ClArray<'a>) (positions: ClArray<int>) ->
            let prefixSumArrayLength = positions.Length

            let resultLengthGpu = clContext.CreateClArray<_>(1)

            let _, r =
                sum processor workGroupSize positions resultLengthGpu

            let resultLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            let resultRows =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let resultColumns =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let resultValues =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let ndRange =
                Range1D(Utils.getDefaultGlobalSize positions.Length, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.SetArguments
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

    let private preparePositions<'a when 'a: struct>
        (clContext: ClContext)
        (opAdd: Expr<'a -> 'a -> 'a>)
        workGroupSize
        =

        let preparePositions =
            <@ fun (ndRange: Range1D) length (allRowsBuffer: ClArray<int>) (allColumnsBuffer: ClArray<int>) (allValuesBuffer: ClArray<'a>) (rawPositionsBuffer: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if (i < length - 1
                    && allRowsBuffer.[i] = allRowsBuffer.[i + 1]
                    && allColumnsBuffer.[i] = allColumnsBuffer.[i + 1]) then
                    rawPositionsBuffer.[i] <- 0
                    allValuesBuffer.[i + 1] <- (%opAdd) allValuesBuffer.[i] allValuesBuffer.[i + 1]
                else
                    (rawPositionsBuffer.[i] <- 1) @>

        let kernel =
            clContext.CreateClKernel(preparePositions)

        fun (processor: MailboxProcessor<_>) (allRows: ClArray<int>) (allColumns: ClArray<int>) (allValues: ClArray<'a>) ->
            let length = allValues.Length

            let ndRange =
                Range1D(Utils.getDefaultGlobalSize (length - 1), workGroupSize)

            let rawPositionsGpu =
                clContext.CreateClArray<int>(length, hostAccessMode = HostAccessMode.NotAccessible)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.SetArguments ndRange length allRows allColumns allValues rawPositionsGpu)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
            rawPositionsGpu

    let private merge<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let merge =
            <@ fun (ndRange: Range1D) firstSide secondSide sumOfSides (firstRowsBuffer: ClArray<int>) (firstColumnsBuffer: ClArray<int>) (firstValuesBuffer: ClArray<'a>) (secondRowsBuffer: ClArray<int>) (secondColumnsBuffer: ClArray<int>) (secondValuesBuffer: ClArray<'a>) (allRowsBuffer: ClArray<int>) (allColumnsBuffer: ClArray<int>) (allValuesBuffer: ClArray<'a>) ->

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

                barrier ()

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

                barrier ()

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
                        allValuesBuffer.[i] <- secondValuesBuffer.[i - localID - beginIdx + boundaryY]
                    else
                        allRowsBuffer.[i] <- int (fstIdx >>> 32)
                        allColumnsBuffer.[i] <- int fstIdx
                        allValuesBuffer.[i] <- firstValuesBuffer.[beginIdx + boundaryX] @>

        let kernel = clContext.CreateClKernel(merge)

        fun (processor: MailboxProcessor<_>) (matrixLeftRows: ClArray<int>) (matrixLeftColumns: ClArray<int>) (matrixLeftValues: ClArray<'a>) (matrixRightRows: ClArray<int>) (matrixRightColumns: ClArray<int>) (matrixRightValues: ClArray<'a>) ->

            let firstSide = matrixLeftValues.Length
            let secondSide = matrixRightValues.Length
            let sumOfSides = firstSide + secondSide

            let allRows =
                clContext.CreateClArray<int>(
                    sumOfSides,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let allColumns =
                clContext.CreateClArray<int>(
                    sumOfSides,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let allValues =
                clContext.CreateClArray<'a>(
                    sumOfSides,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let ndRange =
                Range1D(Utils.getDefaultGlobalSize sumOfSides, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.SetArguments
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
                            allValues)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allRows, allColumns, allValues


    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a -> 'a -> 'a>) workGroupSize =

        let merge = merge clContext workGroupSize

        let preparePositions =
            preparePositions clContext opAdd workGroupSize

        let setPositions = setPositions<'a> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: COOMatrix<'a>) (matrixRight: COOMatrix<'a>) ->

            let allRows, allColumns, allValues =
                merge
                    queue
                    matrixLeft.Rows
                    matrixLeft.Columns
                    matrixLeft.Values
                    matrixRight.Rows
                    matrixRight.Columns
                    matrixRight.Values


            use rawPositions =
                preparePositions queue allRows allColumns allValues

            let resultRows, resultColumns, resultValues, resultLength =
                setPositions queue allRows allColumns allValues rawPositions

            queue.Post(Msg.CreateFreeMsg<_>(allRows))
            queue.Post(Msg.CreateFreeMsg<_>(allColumns))
            queue.Post(Msg.CreateFreeMsg<_>(allValues))

            { RowCount = matrixLeft.RowCount
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


            { RowIndices = resultRows
              ColumnIndices = resultColumns
              Values = resultValues }