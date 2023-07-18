namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContextExtensions
open GraphBLAS.FSharp.Backend.Matrix.COO

module internal Map2 =
    let preparePositions<'a, 'b, 'c> opAdd (clContext: ClContext) workGroupSize =

        let preparePositions (op: Expr<'a option -> 'b option -> 'c option>) =
            <@ fun (ndRange: Range1D) rowCount columnCount (leftValues: ClArray<'a>) (leftRowPointers: ClArray<int>) (leftColumns: ClArray<int>) (rightValues: ClArray<'b>) (rightRowPointers: ClArray<int>) (rightColumn: ClArray<int>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'c>) (resultRows: ClArray<int>) (resultColumns: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < rowCount * columnCount then

                    let columnIndex = gid % columnCount
                    let rowIndex = gid / columnCount

                    let leftStartIndex = leftRowPointers.[rowIndex]
                    let leftLastIndex = leftRowPointers.[rowIndex + 1] - 1

                    let rightStartIndex = rightRowPointers.[rowIndex]
                    let rightLastIndex = rightRowPointers.[rowIndex + 1] - 1

                    let leftValue =
                        (%Search.Bin.inRange) leftStartIndex leftLastIndex columnIndex leftColumns leftValues

                    let rightValue =
                        (%Search.Bin.inRange) rightStartIndex rightLastIndex columnIndex rightColumn rightValues

                    match (%op) leftValue rightValue with
                    | Some value ->
                        resultValues.[gid] <- value
                        resultRows.[gid] <- rowIndex
                        resultColumns.[gid] <- columnIndex

                        resultBitmap.[gid] <- 1
                    | None -> resultBitmap.[gid] <- 0 @>

        let kernel =
            clContext.Compile <| preparePositions opAdd

        fun (processor: MailboxProcessor<_>) rowCount columnCount (leftValues: ClArray<'a>) (leftRows: ClArray<int>) (leftColumns: ClArray<int>) (rightValues: ClArray<'b>) (rightRows: ClArray<int>) (rightColumns: ClArray<int>) ->

            let (resultLength: int) = columnCount * rowCount

            let resultBitmap =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, resultLength)

            let resultRows =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, resultLength)

            let resultColumns =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, resultLength)

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'c>(DeviceOnly, resultLength)

            let ndRange =
                Range1D.CreateValid(resultLength, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            rowCount
                            columnCount
                            leftValues
                            leftRows
                            leftColumns
                            rightValues
                            rightRows
                            rightColumns
                            resultBitmap
                            resultValues
                            resultRows
                            resultColumns)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            resultBitmap, resultValues, resultRows, resultColumns

    ///<param name="clContext">.</param>
    ///<param name="opAdd">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =

        let map2 =
            preparePositions opAdd clContext workGroupSize

        let setPositions =
            Common.setPositions<'c> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->

            let bitmap, values, rows, columns =
                map2
                    queue
                    matrixLeft.RowCount
                    matrixLeft.ColumnCount
                    matrixLeft.Values
                    matrixLeft.RowPointers
                    matrixLeft.Columns
                    matrixRight.Values
                    matrixRight.RowPointers
                    matrixRight.Columns

            let resultRows, resultColumns, resultValues, _ =
                setPositions queue allocationMode rows columns values bitmap

            queue.Post(Msg.CreateFreeMsg<_>(bitmap))
            queue.Post(Msg.CreateFreeMsg<_>(values))
            queue.Post(Msg.CreateFreeMsg<_>(rows))
            queue.Post(Msg.CreateFreeMsg<_>(columns))

            { Context = clContext
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              Rows = resultRows
              Columns = resultColumns
              Values = resultValues }

    module AtLeastOne =
        let preparePositions<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
            (opAdd: Expr<'a option -> 'b option -> 'c option>)
            (clContext: ClContext)
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

        let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
            (opAdd: Expr<'a option -> 'b option -> 'c option>)
            (clContext: ClContext)
            workGroupSize
            =

            let merge =
                GraphBLAS.FSharp.Backend.Matrix.CSR.Merge.run clContext workGroupSize

            let preparePositions =
                preparePositions opAdd clContext workGroupSize

            let setPositions =
                Common.setPositions<'c> clContext workGroupSize

            fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSR<'b>) ->

                let allRows, allColumns, leftMergedValues, rightMergedValues, isRowEnd, isLeft =
                    merge queue matrixLeft matrixRight

                let positions, allValues =
                    preparePositions queue allColumns leftMergedValues rightMergedValues isRowEnd isLeft

                queue.Post(Msg.CreateFreeMsg<_>(leftMergedValues))
                queue.Post(Msg.CreateFreeMsg<_>(rightMergedValues))

                let resultRows, resultColumns, resultValues, _ =
                    setPositions queue allocationMode allRows allColumns allValues positions

                queue.Post(Msg.CreateFreeMsg<_>(allRows))
                queue.Post(Msg.CreateFreeMsg<_>(isLeft))
                queue.Post(Msg.CreateFreeMsg<_>(isRowEnd))
                queue.Post(Msg.CreateFreeMsg<_>(positions))
                queue.Post(Msg.CreateFreeMsg<_>(allColumns))
                queue.Post(Msg.CreateFreeMsg<_>(allValues))

                { Context = clContext
                  RowCount = matrixLeft.RowCount
                  ColumnCount = matrixLeft.ColumnCount
                  Rows = resultRows
                  Columns = resultColumns
                  Values = resultValues }
