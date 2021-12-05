namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined

module internal rec PrepareMatrix =
    let run
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>) =

        let positions = preparePositions clContext workGroupSize processor matrixLeft matrixRight

        let resultNNZGpu = clContext.CreateClArray(1)

        PrefixSum.standardExcludeInplace clContext workGroupSize processor positions resultNNZGpu
        |> ignore

        let resultLength =
            let res = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(resultNNZGpu, [|0|], ch))
            processor.Post(Msg.CreateFreeMsg<_>(resultNNZGpu))
            res.[0]

        let resultColumns, resultValues = setColumnsAndValues clContext workGroupSize processor matrixLeft positions resultLength
        setRowPointers clContext workGroupSize processor matrixLeft positions

        processor.Post(Msg.CreateFreeMsg(positions))

        {
            RowCount = matrixLeft.RowCount
            ColumnCount = matrixLeft.ColumnCount
            RowPointers = matrixLeft.RowPointers
            Columns = resultColumns
            Values = resultValues
        }

    let private preparePositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>) =

        let rawPositions =
            clContext.CreateClArray(
                matrixLeft.Columns.Length + 1,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let length = rawPositions.Length

        let preparePositions =
            <@
                fun (ndRange: Range1D)
                    (firstColumns: ClArray<int>)
                    (secondRowPointers: ClArray<int>)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0
                    if i < length then
                        let column = firstColumns.[i]
                        if i = length - 1 || secondRowPointers.[column] = secondRowPointers.[column + 1] then
                            positions.[i] <- 0
                        else
                            positions.[i] <- 1
            @>

        let kernel = clContext.CreateClKernel(preparePositions)

        let ndRange = Range1D.CreateValid(length, workGroupSize)

        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixLeft.Columns
                        matrixRight.RowPointers
                        rawPositions)
        )

        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        rawPositions

    let private setColumnsAndValues
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (positions: ClArray<int>)
        (length: int) =

        let resultColumns =
            clContext.CreateClArray(
                length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let resultValues =
            clContext.CreateClArray(
                length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let length = matrixLeft.Columns.Length

        let setPositions =
            <@
                fun (ndRange: Range1D)
                    (columns: ClArray<int>)
                    (values: ClArray<'a>)
                    (resultColumns: ClArray<int>)
                    (resultValues: ClArray<'a>)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0
                    if i < length then
                        let index = positions.[i]
                        if index <> positions.[i + 1] then
                            resultColumns.[index] <- columns.[i]
                            resultValues.[index] <- values.[i]
            @>

        let kernel = clContext.CreateClKernel(setPositions)

        let ndRange = Range1D.CreateValid(length, workGroupSize)

        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixLeft.Columns
                        matrixLeft.Values
                        resultColumns
                        resultValues
                        positions)
        )

        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        resultColumns, resultValues

    let private setRowPointers
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (positions: ClArray<int>) =

        let length = matrixLeft.RowPointers.Length

        let setPositions =
            <@
                fun (ndRange: Range1D)
                    (rowPointers: ClArray<int>)
                    (positions: ClArray<int>) ->

                    let i = ndRange.GlobalID0
                    if i < length then
                        rowPointers.[i] <- positions.[rowPointers.[i]]
            @>

        let kernel = clContext.CreateClKernel(setPositions)

        let ndRange = Range1D.CreateValid(length, workGroupSize)

        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixLeft.RowPointers
                        positions)
        )

        processor.Post(Msg.CreateRunMsg<_, _>(kernel))


