namespace GraphBLAS.FSharp.Backend.Matrix.COO

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Matrix
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Quotes

module internal Map2 =

    let preparePositions<'a, 'b, 'c> (clContext: ClContext) workGroupSize opAdd =

        let preparePositions (op: Expr<'a option -> 'b option -> 'c option>) =
            <@ fun (ndRange: Range1D) rowCount columnCount leftValuesLength rightValuesLength (leftValues: ClArray<'a>) (leftRows: ClArray<int>) (leftColumns: ClArray<int>) (rightValues: ClArray<'b>) (rightRows: ClArray<int>) (rightColumn: ClArray<int>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'c>) (resultRows: ClArray<int>) (resultColumns: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < rowCount * columnCount then

                    let columnIndex = gid % columnCount
                    let rowIndex = gid / columnCount

                    let index =
                        (uint64 rowIndex <<< 32) ||| (uint64 columnIndex)

                    let leftValue =
                        (%Search.Bin.byKey2D) leftValuesLength index leftRows leftColumns leftValues

                    let rightValue =
                        (%Search.Bin.byKey2D) rightValuesLength index rightRows rightColumn rightValues

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
                            leftValues.Length
                            rightValues.Length
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
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let map2 =
            preparePositions clContext workGroupSize opAdd

        let setPositions =
            Common.setPositions<'c> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrixLeft: ClMatrix.COO<'a>) (matrixRight: ClMatrix.COO<'b>) ->

            let bitmap, values, rows, columns =
                map2
                    queue
                    matrixLeft.RowCount
                    matrixLeft.ColumnCount
                    matrixLeft.Values
                    matrixLeft.Rows
                    matrixLeft.Columns
                    matrixRight.Values
                    matrixRight.Rows
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
