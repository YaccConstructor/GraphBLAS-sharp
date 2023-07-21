namespace GraphBLAS.FSharp.Backend.Matrix.COO

open System
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Objects.ClMatrix
open GraphBLAS.FSharp.Objects.ClContextExtensions

module internal Map =
    let preparePositions<'a, 'b> opAdd (clContext: ClContext) workGroupSize =

        let preparePositions (op: Expr<'a option -> 'b option>) =
            <@ fun (ndRange: Range1D) rowCount columnCount valuesLength (values: ClArray<'a>) (rows: ClArray<int>) (columns: ClArray<int>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'b>) (resultRows: ClArray<int>) (resultColumns: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < rowCount * columnCount then

                    let columnIndex = gid % columnCount
                    let rowIndex = gid / columnCount

                    let index =
                        (uint64 rowIndex <<< 32) ||| (uint64 columnIndex)

                    let value =
                        (%Search.Bin.byKey2D) valuesLength index rows columns values

                    match (%op) value with
                    | Some resultValue ->
                        resultValues.[gid] <- resultValue
                        resultRows.[gid] <- rowIndex
                        resultColumns.[gid] <- columnIndex

                        resultBitmap.[gid] <- 1
                    | None -> resultBitmap.[gid] <- 0 @>

        let kernel =
            clContext.Compile <| preparePositions opAdd

        fun (processor: MailboxProcessor<_>) rowCount columnCount (values: ClArray<'a>) (rowPointers: ClArray<int>) (columns: ClArray<int>) ->

            let (resultLength: int) = columnCount * rowCount

            let resultBitmap =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, resultLength)

            let resultRows =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, resultLength)

            let resultColumns =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, resultLength)

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'b>(DeviceOnly, resultLength)

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
                            values.Length
                            values
                            rowPointers
                            columns
                            resultBitmap
                            resultValues
                            resultRows
                            resultColumns)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            resultBitmap, resultValues, resultRows, resultColumns

    let run<'a, 'b when 'a: struct and 'b: struct and 'b: equality>
        (op: Expr<'a option -> 'b option>)
        (clContext: ClContext)
        workGroupSize
        =

        let map =
            preparePositions op clContext workGroupSize

        let setPositions =
            Common.setPositions<'b> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.COO<'a>) ->

            let bitmap, values, rows, columns =
                map queue matrix.RowCount matrix.ColumnCount matrix.Values matrix.Rows matrix.Columns

            let resultRows, resultColumns, resultValues, _ =
                setPositions queue allocationMode rows columns values bitmap

            queue.Post(Msg.CreateFreeMsg<_>(bitmap))
            queue.Post(Msg.CreateFreeMsg<_>(values))
            queue.Post(Msg.CreateFreeMsg<_>(rows))
            queue.Post(Msg.CreateFreeMsg<_>(columns))

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = resultRows
              Columns = resultColumns
              Values = resultValues }
