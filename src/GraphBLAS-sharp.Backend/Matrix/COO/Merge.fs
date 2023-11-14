namespace GraphBLAS.FSharp.Backend.Matrix.COO

open Brahma.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ClMatrix
open GraphBLAS.FSharp.Objects.ArraysExtensions

module Merge =
    let run<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) workGroupSize =

        let merge =
            <@ fun (ndRange: Range1D) firstSide secondSide sumOfSides (firstRowsBuffer: ClArray<int>) (firstColumnsBuffer: ClArray<int>) (firstValuesBuffer: ClArray<'a>) (secondRowsBuffer: ClArray<int>) (secondColumnsBuffer: ClArray<int>) (secondValuesBuffer: ClArray<'b>) (allRowsBuffer: ClArray<int>) (allColumnsBuffer: ClArray<int>) (leftMergedValuesBuffer: ClArray<'a>) (rightMergedValuesBuffer: ClArray<'b>) (isLeftBitmap: ClArray<int>) ->

                let i = ndRange.GlobalID0

                let mutable beginIdxLocal = local ()
                let mutable endIdxLocal = local ()
                let localID = ndRange.LocalID0

                if localID < 2 then
                    let x = localID * (workGroupSize - 1) + i - 1

                    let diagonalNumber = min (sumOfSides - 1) x

                    let mutable leftEdge = diagonalNumber + 1 - secondSide
                    leftEdge <- max 0 leftEdge

                    let mutable rightEdge = firstSide - 1

                    rightEdge <- min diagonalNumber rightEdge

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
                    leftEdge <- max 0 leftEdge

                    let mutable rightEdge = firstLocalLength - 1

                    rightEdge <- min localID rightEdge

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

        fun (processor: MailboxProcessor<_>) (leftMatrix: ClMatrix.COO<'a>) (rightMatrix: ClMatrix.COO<'b>) ->

            let firstSide = leftMatrix.Columns.Length
            let secondSide = rightMatrix.Columns.Length
            let sumOfSides = firstSide + secondSide

            let allRows =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, sumOfSides)

            let allColumns =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, sumOfSides)

            let leftMergedValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(DeviceOnly, sumOfSides)

            let rightMergedValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'b>(DeviceOnly, sumOfSides)

            let isLeft =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, sumOfSides)

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
                            leftMatrix.Rows
                            leftMatrix.Columns
                            leftMatrix.Values
                            rightMatrix.Rows
                            rightMatrix.Columns
                            rightMatrix.Values
                            allRows
                            allColumns
                            leftMergedValues
                            rightMergedValues
                            isLeft)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allRows, allColumns, leftMergedValues, rightMergedValues, isLeft

    let runDisjoint<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let mergeValues =
            <@ fun (ndRange: Range1D) (length: int) (leftValues: ClArray<'a>) (rightValues: ClArray<'a>) (isLeft: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < length then

                    if isLeft.[gid] = 0 then
                        leftValues.[gid] <- rightValues.[gid] @>

        let mergeValuesKernel = clContext.Compile(mergeValues)

        let merge = run clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftMatrix: ClMatrix.COO<'a>) (rightMatrix: ClMatrix.COO<'a>) ->

            let length = leftMatrix.Columns.Length + rightMatrix.Columns.Length

            let rows, cols, leftValues, rightValues, isLeft = merge processor leftMatrix rightMatrix

            let ndRange = Range1D.CreateValid(length, workGroupSize)

            let mergeValuesKernel = mergeValuesKernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        mergeValuesKernel.KernelFunc
                            ndRange
                            length
                            leftValues
                            rightValues
                            isLeft)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(mergeValuesKernel))

            isLeft.Free processor
            rightValues.Free processor

            { Context = clContext
              Rows = rows
              Columns = cols
              Values = leftValues
              ColumnCount = leftMatrix.ColumnCount
              RowCount = leftMatrix.RowCount }
