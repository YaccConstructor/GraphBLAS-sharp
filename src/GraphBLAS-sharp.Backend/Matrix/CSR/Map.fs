﻿namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Matrix.COO
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContext

module internal Map =
    let preparePositions<'a, 'b> (clContext: ClContext) workGroupSize op =

        let preparePositions (op: Expr<'a option -> 'b option>) =
            <@ fun (ndRange: Range1D) rowCount columnCount (values: ClArray<'a>) (rowPointers: ClArray<int>) (columns: ClArray<int>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'b>) (resultRows: ClArray<int>) (resultColumns: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < rowCount * columnCount then

                    let columnIndex = gid % columnCount
                    let rowIndex = gid / columnCount

                    let startIndex = rowPointers.[rowIndex]
                    let lastIndex = rowPointers.[rowIndex + 1] - 1

                    let value =
                        (%Search.Bin.inRange) startIndex lastIndex columnIndex columns values

                    match (%op) value with
                    | Some resultValue ->
                        resultValues.[gid] <- resultValue
                        resultRows.[gid] <- rowIndex
                        resultColumns.[gid] <- columnIndex

                        resultBitmap.[gid] <- 1
                    | None -> resultBitmap.[gid] <- 0 @>

        let kernel = clContext.Compile <| preparePositions op

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

    let runToCOO<'a, 'b when 'a: struct and 'b: struct and 'b: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option>)
        workGroupSize
        =

        let map =
            preparePositions clContext workGroupSize opAdd

        let setPositions =
            Common.setPositions<'b> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->

            let bitmap, values, rows, columns =
                map queue matrix.RowCount matrix.ColumnCount matrix.Values matrix.RowPointers matrix.Columns

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

    let run<'a, 'b when 'a: struct and 'b: struct and 'b: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option>)
        workGroupSize
        =

        let mapToCOO = runToCOO clContext opAdd workGroupSize

        let toCSRInplace =
            Matrix.toCSRInPlace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            mapToCOO queue allocationMode matrix
            |> toCSRInplace queue allocationMode