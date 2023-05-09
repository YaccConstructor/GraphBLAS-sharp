namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContext

module internal Map =
    let preparePositions<'a, 'b> op (clContext: ClContext) workGroupSize =

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

    let run<'a, 'b when 'a: struct and 'b: struct and 'b: equality>
        (opAdd: Expr<'a option -> 'b option>)
        (clContext: ClContext)
        workGroupSize
        =

        let map =
            preparePositions opAdd clContext workGroupSize

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

    module WithValue =
        let preparePositions<'a, 'b, 'c when 'b: struct> (clContext: ClContext) workGroupSize op =

            let preparePositions (op: Expr<'a option -> 'b option -> 'c option>) =
                <@ fun (ndRange: Range1D) (operand: ClCell<'a option>) rowCount columnCount (values: ClArray<'b>) (rowPointers: ClArray<int>) (columns: ClArray<int>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'c>) (resultRows: ClArray<int>) (resultColumns: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid < rowCount * columnCount then

                        let columnIndex = gid % columnCount
                        let rowIndex = gid / columnCount

                        let startIndex = rowPointers.[rowIndex]
                        let lastIndex = rowPointers.[rowIndex + 1] - 1

                        let value =
                            (%Search.Bin.inRange) startIndex lastIndex columnIndex columns values

                        match (%op) operand.Value value with
                        | Some resultValue ->
                            resultValues.[gid] <- resultValue
                            resultRows.[gid] <- rowIndex
                            resultColumns.[gid] <- columnIndex

                            resultBitmap.[gid] <- 1
                        | None -> resultBitmap.[gid] <- 0 @>

            let kernel = clContext.Compile <| preparePositions op

            fun (processor: MailboxProcessor<_>) (operand: ClCell<'a option>) (matrix: ClMatrix.CSR<'b>) ->

                let resultLength = matrix.RowCount * matrix.ColumnCount

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
                                operand
                                matrix.RowCount
                                matrix.ColumnCount
                                matrix.Values
                                matrix.RowPointers
                                matrix.Columns
                                resultBitmap
                                resultValues
                                resultRows
                                resultColumns)
                )

                processor.Post(Msg.CreateRunMsg<_, _> kernel)

                resultBitmap, resultValues, resultRows, resultColumns

        let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
            (clContext: ClContext)
            (op: Expr<'a option -> 'b option -> 'c option>)
            workGroupSize
            =

            let mapWithValue =
                preparePositions clContext workGroupSize op

            let setPositions =
                Common.setPositionsOption<'c> clContext workGroupSize

            fun (queue: MailboxProcessor<_>) allocationMode (value: 'a option) (matrix: ClMatrix.CSR<'b>) ->
                let valueClCell = clContext.CreateClCell value

                let bitmap, values, rows, columns = mapWithValue queue valueClCell matrix

                valueClCell.Free queue

                let result =
                    setPositions queue allocationMode rows columns values bitmap

                queue.Post(Msg.CreateFreeMsg<_>(bitmap))
                queue.Post(Msg.CreateFreeMsg<_>(values))
                queue.Post(Msg.CreateFreeMsg<_>(rows))
                queue.Post(Msg.CreateFreeMsg<_>(columns))

                result
                |> Option.map
                    (fun (resRows, resCols, resValues, _) ->
                        { Context = clContext
                          RowCount = matrix.RowCount
                          ColumnCount = matrix.ColumnCount
                          Rows = resRows
                          Columns = resCols
                          Values = resValues })
