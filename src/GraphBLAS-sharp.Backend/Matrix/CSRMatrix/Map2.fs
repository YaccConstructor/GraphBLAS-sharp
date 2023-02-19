namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open System
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects.ClContext

module internal Map2 =
    let preparePositions<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let preparePositions =
            <@ fun (ndRange: Range1D) length (allColumns: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (allValues: ClArray<'c>) (rawPositions: ClArray<int>) (isEndOfRowBitmap: ClArray<int>) (isLeftBitmap: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if (i < length - 1
                    && allColumns.[i] = allColumns.[i + 1]
                    && isEndOfRowBitmap.[i] = 0) then

                    let result =
                        (%opAdd) (Some leftValues.[i + 1]) (Some rightValues.[i])

                    (%PreparePositions.both) i result rawPositions allValues
                elif i = 0
                     || (i < length
                         && (allColumns.[i] <> allColumns.[i - 1]
                             || isEndOfRowBitmap.[i - 1] = 1)) then

                    let leftResult = (%opAdd) (Some leftValues.[i]) None
                    let rightResult = (%opAdd) None (Some rightValues.[i])

                    (%PreparePositions.leftRight) i leftResult rightResult isLeftBitmap allValues rawPositions @>

        let kernel = clContext.Compile(preparePositions)

        fun (processor: MailboxProcessor<_>) (allColumns: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isEndOfRow: ClArray<int>) (isLeft: ClArray<int>) ->
            let length = leftValues.Length

            let ndRange =
                Range1D.CreateValid(length, workGroupSize)

            let rowPositions =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, length)

            let allValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'c>(DeviceOnly, length)

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
                            rowPositions
                            isEndOfRow
                            isLeft)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
            rowPositions, allValues

    let merge<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) workGroupSize =
        let localArraySize = workGroupSize + 2

        let merge =
            <@ fun (ndRange: Range1D) (firstRowPointers: ClArray<int>) (firstColumns: ClArray<int>) (firstValues: ClArray<'a>) (secondRowPointers: ClArray<int>) (secondColumns: ClArray<int>) (secondValues: ClArray<'b>) (allRows: ClArray<int>) (allColumns: ClArray<int>) (leftMergedValues: ClArray<'a>) (rightMergedValues: ClArray<'b>) (isEndOfRowBitmap: ClArray<int>) (isLeftBitmap: ClArray<int>) ->

                let globalID = ndRange.GlobalID0
                let localID = ndRange.LocalID0
                let MaxVal = Int32.MaxValue

                let row = globalID / workGroupSize

                let firstOffset = firstRowPointers.[row]
                let secondOffset = secondRowPointers.[row]
                let resOffset = firstOffset + secondOffset

                let firstRowEnd = firstRowPointers.[row + 1]
                let secondRowEnd = secondRowPointers.[row + 1]

                let firstRowLength = firstRowEnd - firstOffset
                let secondRowLength = secondRowEnd - secondOffset
                let resRowLength = firstRowLength + secondRowLength

                let workBlockCount =
                    (resRowLength + workGroupSize - 1) / workGroupSize

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

                    let firstBufferSize =
                        min (firstRowLength - firstLocalOffset) workGroupSize

                    let secondBufferSize =
                        min (secondRowLength - secondLocalOffset) workGroupSize

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

                    let workSize =
                        min (firstBufferSize + secondBufferSize) workGroupSize

                    let mutable res = MaxVal

                    let i =
                        if dir then
                            localID
                        else
                            workGroupSize - 1 - localID

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

                            let ans =
                                secondRowLocal.[y - l - mid] > firstRowLocal.[x + l + mid]

                            if ans then
                                l <- l + mid
                            else
                                r <- r - mid

                        let resX = x + l
                        let resY = y - l

                        let outputIndex =
                            resOffset
                            + firstLocalOffset
                            + secondLocalOffset
                            + i

                        if resY = 1
                           || (resX <> 0
                               && secondRowLocal.[resY - 1] <= firstRowLocal.[resX]) then
                            res <- firstRowLocal.[resX]

                            leftMergedValues.[outputIndex] <- firstValues.[firstOffset + firstLocalOffset + resX - 1]

                            isLeftBitmap.[outputIndex] <- 1
                            maxFirstIndexPerThread <- max maxFirstIndexPerThread resX
                        else
                            res <- secondRowLocal.[resY - 1]

                            rightMergedValues.[outputIndex] <-
                                secondValues.[secondOffset + secondLocalOffset + resY - 1 - 1]

                            isLeftBitmap.[outputIndex] <- 0
                            maxSecondIndexPerThread <- max maxSecondIndexPerThread (resY - 1)

                        allRows.[outputIndex] <- row
                        allColumns.[outputIndex] <- res
                        isEndOfRowBitmap.[outputIndex] <- 0

                    //Moving the window of search
                    if block < workBlockCount - 1 then
                        atomic (max) maxFirstIndex maxFirstIndexPerThread
                        |> ignore

                        atomic (max) maxSecondIndex maxSecondIndexPerThread
                        |> ignore

                        barrierFull ()

                        dir <- not dir

                        firstLocalOffset <- firstLocalOffset + maxFirstIndex
                        secondLocalOffset <- secondLocalOffset + maxSecondIndex

                        barrierLocal ()
                    else if i = workSize - 1 then
                        isEndOfRowBitmap.[resOffset
                                          + firstLocalOffset
                                          + secondLocalOffset
                                          + i] <- 1 @>

        let kernel = clContext.Compile(merge)

        fun (processor: MailboxProcessor<_>) (matrixLeftRowPointers: ClArray<int>) (matrixLeftColumns: ClArray<int>) (matrixLeftValues: ClArray<'a>) (matrixRightRowPointers: ClArray<int>) (matrixRightColumns: ClArray<int>) (matrixRightValues: ClArray<'b>) ->

            let firstLength = matrixLeftValues.Length
            let secondLength = matrixRightValues.Length
            let resLength = firstLength + secondLength

            let allRows =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, resLength)

            let allColumns =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, resLength)

            let leftMergedValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(DeviceOnly, resLength)

            let rightMergedValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'b>(DeviceOnly, resLength)

            let isEndOfRow =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, resLength)

            let isLeft =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, resLength)

            let ndRange =
                Range1D.CreateValid((matrixLeftRowPointers.Length - 1) * workGroupSize, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
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
