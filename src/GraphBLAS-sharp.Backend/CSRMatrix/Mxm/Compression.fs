namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

module internal rec Compression =
    let run (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) (plus: Expr<'a -> 'a -> 'a>) (zero: 'a) ->

            let heads, tails = createFlags clContext workGroupSize processor matrix.RowPointers matrix.Columns

            let valuesToScan = ClArray.zip clContext workGroupSize processor matrix.Values heads

            let opAdd =
                <@
                    fun ((x1, x2): struct('a * int))
                        ((y1, y2): struct('a * int)) ->

                        if y2 = 1 then
                            struct(y1, 1)
                        else
                            let buff = (%plus) x1 y1
                            struct(buff, x2)
                @>

            let totalSum = clContext.CreateClArray<struct('a * int)>(1)
            let scannedValues, _ = PrefixSum.runIncludeInplace clContext workGroupSize processor valuesToScan totalSum opAdd struct(zero, 0)

            let resultLength = clContext.CreateClArray<int>(1)
            let positions, resultLength = PrefixSum.runExcludeInplace clContext workGroupSize processor tails resultLength <@ (+) @> 0

            let resultLength =
                let res = [| 0 |]

                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(resultLength, res, ch))

                processor.Post(Msg.CreateFreeMsg<_>(resultLength))

                res.[0]

            let resultColumns, resultValues = setColumnsAndValues clContext workGroupSize processor matrix.Columns scannedValues resultLength positions

            {
                RowCount = matrix.RowCount
                ColumnCount = matrix.ColumnCount
                RowPointers = matrix.RowPointers
                Columns = resultColumns
                Values = resultValues
            }

    let private initFlags (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>) (columns: ClArray<int>) (length: int) ->
            let initFlags =
                <@
                    fun (range: Range1D)
                        (columnsBuffer: ClArray<int>)
                        (headsBuffer: ClArray<int>)
                        (tailsBuffer: ClArray<int>) ->

                        let i = range.GlobalID0

                        if i < length then
                            if i = 0 || columnsBuffer.[i - 1] <> columnsBuffer.[i] then headsBuffer.[i] <- 1 else headsBuffer.[i] <- 0
                            if i = length - 1 || columnsBuffer.[i] <> columnsBuffer.[i + 1] then tailsBuffer.[i] <- 1 else tailsBuffer.[i] <- 0
                @>

            let heads =
                clContext.CreateClArray<int>(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let tails =
                clContext.CreateClArray<int>(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let kernel = clContext.CreateClKernel(initFlags)

            let ndRange = Range1D.CreateValid(length, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            columns
                            heads
                            tails)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            heads, tails

    // TODO: оптимальней ли будет сначала убрать повторяющиеся значения из rowPointers?
    let private createFlags (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>) (rowPointers: ClArray<int>) (columns: ClArray<int>) ->

            let length = columns.Length

            let heads, tails = initFlags clContext workGroupSize processor columns length

            let rowPointersLength = rowPointers.Length

            let updateFlags =
                <@
                    fun (range: Range1D)
                        (rowPointersBuffer: ClArray<int>)
                        (headsBuffer: ClArray<int>)
                        (tailsBuffer: ClArray<int>) ->

                        let i = range.GlobalID0

                        if i < rowPointersLength then
                            let j = rowPointersBuffer.[i]
                            if j < length then headsBuffer.[j] <- 1
                            if j > 0 then tailsBuffer.[j - 1] <- 1
                @>

            let kernel = clContext.CreateClKernel(updateFlags)

            let ndRange = Range1D.CreateValid(rowPointersLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            rowPointers
                            heads
                            tails)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            heads, tails

    let private setColumnsAndValues (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>)
            (columns: ClArray<int>)
            (values: ClArray<struct('a*int)>)
            (length: int)
            (positions: ClArray<int>) ->

            let columnsLength = positions.Length

            let setColumnsAndValues =
                <@
                    fun (ndRange: Range1D)
                        (columnsBuffer: ClArray<int>)
                        (valuesBuffer: ClArray<_>)
                        (resultColumnsBuffer: ClArray<int>)
                        (resultValuesBuffer: ClArray<'a>)
                        (positionsBuffer: ClArray<int>) ->

                        let i = ndRange.GlobalID0

                        if i = columnsLength - 1
                        || i < columnsLength
                        && positionsBuffer.[i] <> positionsBuffer.[i + 1]
                        then
                            let index = positionsBuffer.[i]

                            resultColumnsBuffer.[index] <- columnsBuffer.[i]
                            let struct(buff, _) = valuesBuffer.[i]
                            resultValuesBuffer.[index] <- buff
                @>

            let resultColumns =
                clContext.CreateClArray(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let resultValues =
                clContext.CreateClArray(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let kernel = clContext.CreateClKernel(setColumnsAndValues)

            let ndRange = Range1D.CreateValid(positions.Length, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            columns
                            values
                            resultColumns
                            resultValues
                            positions)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultColumns, resultValues

    // let private setRowPointers (clContext: ClContext) workGroupSize =
    //     fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) (positions: ClArray<int>) ->
    //         let rowPointersLength = matrix.RowPointers.Length

    //         let setRowPointers =
    //             <@
    //                 fun (ndRange: Range1D)
    //                     (rowPointersBuffer: ClArray<int>)
    //                     (positionsBuffer: ClArray<int>) ->

    //                     let i = ndRange.GlobalID0
    //                     if i < rowPointersLength then rowPointersBuffer.[i] <- positionsBuffer.[rowPointersBuffer.[i]]
    //             @>

    //         let kernel = clContext.CreateClKernel(setRowPointers)

    //         let ndRange = Range1D.CreateValid(rowPointersLength, workGroupSize)

    //         processor.Post(
    //             Msg.MsgSetArguments
    //                 (fun () ->
    //                     kernel.ArgumentsSetter
    //                         ndRange
    //                         matrix.RowPointers
    //                         positions)
    //         )

    //         processor.Post(Msg.CreateRunMsg<_, _>(kernel))


    // let private preparePositions (clContext: ClContext) workGroupSize =
    //     let preparePositions =
    //         <@
    //             fun (ndRange: Range1D)
    //                 (rowPointersBuffer: ClArray<int>)
    //                 (columnsBuffer: ClArray<int>)
    //                 (rawPositionsBuffer: ClArray<int>) ->

    //                 let i = ndRange.GlobalID0
    //                 let localID = ndRange.LocalID0
    //                 let workGroupNumber = i / workGroupSize

    //                 let beginIndex = rowPointersBuffer.[workGroupNumber]
    //                 let mutable j = localID + beginIndex
    //                 let endIndex = rowPointersBuffer.[workGroupNumber + 1]
    //                 while j < endIndex do
    //                     let currColumn = columnsBuffer.[j]
    //                     if j < endIndex - 1 && currColumn = columnsBuffer.[j + 1] then rawPositionsBuffer.[j] <- 0 else rawPositionsBuffer.[j] <- 1
    //                     j <- j + workGroupSize
    //         @>

    //     fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) ->
    //         let rawPositions =
    //             clContext.CreateClArray(
    //                 matrix.Columns.Length + 1,
    //                 hostAccessMode = HostAccessMode.NotAccessible,
    //                 deviceAccessMode = DeviceAccessMode.WriteOnly
    //             )

    //         let kernel = clContext.CreateClKernel(preparePositions)

    //         let ndRange = Range1D(workGroupSize * matrix.RowCount)

    //         processor.Post(
    //             Msg.MsgSetArguments
    //                 (fun () ->
    //                     kernel.ArgumentsSetter
    //                         ndRange
    //                         matrix.RowPointers
    //                         matrix.Columns
    //                         rawPositions)
    //         )

    //         processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    //         rawPositions
