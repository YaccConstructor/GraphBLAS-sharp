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

        let resultColumns = Scatter.run clContext workGroupSize processor positions resultLength matrixLeft.Columns
        let resultValues = Scatter.run clContext workGroupSize processor positions resultLength matrixLeft.Values
        let resultRowPointers = ClArray.copy clContext processor workGroupSize matrixLeft.RowPointers
        ScatterRowPointers.runInPlace clContext workGroupSize processor positions matrixLeft.Columns.Length resultLength resultRowPointers

        processor.Post(Msg.CreateFreeMsg(positions))

        {
            RowCount = matrixLeft.RowCount
            ColumnCount = matrixLeft.ColumnCount
            RowPointers = resultRowPointers
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
                matrixLeft.Columns.Length,
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
                        if secondRowPointers.[column] = secondRowPointers.[column + 1] then
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
