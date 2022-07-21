namespace GraphBLAS.FSharp.Backend

open System.Diagnostics.CodeAnalysis
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module EWise =
    let private copyWithOffset (clContext: ClContext) workGroupSize =

        let copyWithOffsetLeft =
            <@ fun (ndRange: Range1D) rowSize srcOffset dstOffset (sourceCols: ClArray<int>) (sourceValues: ClArray<'a>) (destinationCols: ClArray<int>) (destinationValues: ClArray<'a>) (isLeftBitmap: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < rowSize
                    then
                        destinationCols.[dstOffset + i] <- sourceCols.[srcOffset + i]
                        destinationValues.[dstOffset + i] <- sourceValues.[srcOffset + i]
                        isLeftBitmap.[dstOffset + i] <- 1 @>

        let copyWithOffsetRight =
            <@ fun (ndRange: Range1D) rowSize srcOffset dstOffset (sourceCols: ClArray<int>) (sourceValues: ClArray<'a>) (destinationCols: ClArray<int>) (destinationValues: ClArray<'a>) (isLeftBitmap: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < rowSize
                    then
                        destinationCols.[dstOffset + i] <- sourceCols.[srcOffset + i]
                        destinationValues.[dstOffset + i] <- sourceValues.[srcOffset + i]
                        isLeftBitmap.[dstOffset + i] <- 0 @>

        let kernelLeft = clContext.Compile(copyWithOffsetLeft)
        let kernelRight = clContext.Compile(copyWithOffsetRight)

        fun (processor: MailboxProcessor<_>) rowSize srcOffset dstOffset isLeft (sourceCols: ClArray<int>) (sourceValues: ClArray<'a>) (destinationCols: ClArray<int>) (destinationValues: ClArray<'a>) (isLeftBitmap: ClArray<int>) ->

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
                                    isLeftBitmap)
                    )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let merge<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) workGroupSize =

        let merge =
            <@ fun (ndRange: Range1D) firstOffset secondOffset firstSide secondSide sumOfSides (firstRowPointers: ClArray<int>) (firstColumnsBuffer: ClArray<int>) (firstValuesBuffer: ClArray<'a>) (secondRowPointers: ClArray<int>) (secondColumnsBuffer: ClArray<int>) (secondValuesBuffer: ClArray<'b>) (allColumnsBuffer: ClArray<int>) (leftMergedValuesBuffer: ClArray<'a>) (rightMergedValuesBuffer: ClArray<'b>) (isLeftBitmap: ClArray<int>) ->

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

                            let firstIndex: uint64 = (uint64 firstColumnsBuffer.[firstOffset + middleIdx])

                            let secondIndex: uint64 = (uint64 secondColumnsBuffer.[secondOffset + diagonalNumber - middleIdx])

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
                        localIndices.[localID] <- (uint64 firstColumnsBuffer.[firstOffset + beginIdx + localID])

                    if localID < secondLocalLength then
                        localIndices.[firstLocalLength + localID] <- (uint64 secondColumnsBuffer.[secondOffset + i - beginIdx])

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
                            allColumnsBuffer.[firstOffset + secondOffset + i] <- int sndIdx
                            rightMergedValuesBuffer.[firstOffset + secondOffset + i] <- secondValuesBuffer.[secondOffset + i - localID - beginIdx + boundaryY]
                            isLeftBitmap.[i] <- 0
                        else
                            allColumnsBuffer.[firstOffset + secondOffset + i] <- int fstIdx
                            leftMergedValuesBuffer.[firstOffset + secondOffset + i] <- firstValuesBuffer.[firstOffset + beginIdx + boundaryX]
                            isLeftBitmap.[i] <- 1 @>

        let copyWithOffsetLeft = copyWithOffset clContext workGroupSize
        let copyWithOffsetRight = copyWithOffset clContext workGroupSize

        let kernel = clContext.Compile(merge)

        fun (processor: MailboxProcessor<_>) (matrixLeftRowPointers: ClArray<int>) (matrixLeftColumns: ClArray<int>) (matrixLeftValues: ClArray<'a>) (matrixRightRowPointers: ClArray<int>) (matrixRightColumns: ClArray<int>) (matrixRightValues: ClArray<'b>) ->

            let firstSide = matrixLeftValues.Length
            let secondSide = matrixRightValues.Length
            let sumOfSides = firstSide + secondSide

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

            //Transfering row pointers to CPU to calculate offsets
            let leftRowPointersCPU = Array.zeroCreate matrixLeftRowPointers.Length
            let rightRowPointersCPU = Array.zeroCreate matrixLeftRowPointers.Length

            processor.Post(Msg.CreateToHostMsg(matrixLeftRowPointers, leftRowPointersCPU))
            processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(matrixRightRowPointers, rightRowPointersCPU, ch))

            //Merge on each row
            for row in 0 .. matrixLeftRowPointers.Length - 1 do
                let leftOffset = leftRowPointersCPU.[row]
                let rightOffset = rightRowPointersCPU.[row]
                let resOffset = leftOffset + rightOffset
                let leftBorder = if row = matrixLeftRowPointers.Length - 1 then matrixLeftValues.Length else leftRowPointersCPU.[row + 1]
                let rightBorder = if row = matrixRightRowPointers.Length - 1 then matrixRightValues.Length else rightRowPointersCPU.[row + 1]

                let leftRowSize = leftBorder - leftOffset
                let rightRowSize = rightBorder - rightOffset

                let resRowSize = leftRowSize + rightRowSize

                if resRowSize > 0 then
                    if leftRowSize = 0 then copyWithOffsetRight processor rightRowSize rightOffset resOffset false matrixRightColumns matrixRightValues allColumns rightMergedValues isLeft
                    elif rightRowSize = 0 then copyWithOffsetLeft processor leftRowSize leftOffset resOffset true matrixLeftColumns matrixLeftValues allColumns leftMergedValues isLeft
                    else
                        let ndRange =
                            Range1D.CreateValid(resRowSize, workGroupSize)

                        let kernel = kernel.GetKernel()

                        processor.Post(
                            Msg.MsgSetArguments
                                (fun () ->
                                    kernel.KernelFunc
                                        ndRange
                                        leftOffset
                                        rightOffset
                                        leftRowSize
                                        rightRowSize
                                        resRowSize
                                        matrixLeftRowPointers
                                        matrixLeftColumns
                                        matrixLeftValues
                                        matrixRightRowPointers
                                        matrixRightColumns
                                        matrixRightValues
                                        allColumns
                                        leftMergedValues
                                        rightMergedValues
                                        isLeft)
                        )

                        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            processor.PostAndReply(Msg.MsgNotifyMe)
            let cols = Array.zeroCreate sumOfSides
            processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(allColumns, cols, ch))
            printfn "%A" cols

            allColumns, leftMergedValues, rightMergedValues, isLeft
