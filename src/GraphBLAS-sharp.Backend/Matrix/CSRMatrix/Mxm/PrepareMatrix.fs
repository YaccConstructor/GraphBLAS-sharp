namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined

module internal PrepareMatrix =
    let private preparePositions
        (clContext: ClContext)
        workGroupSize =

        let preparePositions =
            <@
                fun (ndRange: Range1D)
                    (firstColumns: ClArray<int>)
                    (secondRowPointers: ClArray<int>)
                    (positions: ClArray<int>)
                    (length: int) ->

                    let i = ndRange.GlobalID0
                    if i < length then
                        let column = firstColumns.[i]
                        if secondRowPointers.[column] = secondRowPointers.[column + 1] then
                            positions.[i] <- 0
                        else
                            positions.[i] <- 1
            @>
        let kernel = clContext.CreateClKernel(preparePositions)

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>) ->

            let rawPositions =
                clContext.CreateClArray(
                    matrixLeft.Columns.Length,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let length = rawPositions.Length

            let ndRange = Range1D.CreateValid(length, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            matrixLeft.Columns
                            matrixRight.RowPointers
                            rawPositions
                            length)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            rawPositions

    let run
        (clContext: ClContext)
        workGroupSize =

        let preparePositions = preparePositions clContext workGroupSize
        let scanExcludeInPlace = PrefixSum.standardExcludeInplace clContext workGroupSize
        let scatter = Scatter.arrayInPlace clContext workGroupSize
        let scatterData = Scatter.arrayInPlace clContext workGroupSize
        let scatterRowPointers = ScatterRowPointers.runInPlace clContext workGroupSize
        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>) ->

            let positions = preparePositions processor matrixLeft matrixRight

            let resultNNZGpu = clContext.CreateClCell()

            scanExcludeInPlace processor positions resultNNZGpu
            |> ignore
            let resultLength =
                ClCell.toHost resultNNZGpu
                |> ClTask.runSync clContext
            processor.Post(Msg.CreateFreeMsg<_>(resultNNZGpu))

            let resultColumns =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let resultValues =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            scatter processor positions matrixLeft.Columns resultColumns
            scatterData processor positions matrixLeft.Values resultValues
            let resultRowPointers = copy processor matrixLeft.RowPointers
            scatterRowPointers processor positions matrixLeft.Columns.Length resultLength resultRowPointers

            processor.Post(Msg.CreateFreeMsg(positions))

            {
                Context = clContext
                RowCount = matrixLeft.RowCount
                ColumnCount = matrixLeft.ColumnCount
                RowPointers = resultRowPointers
                Columns = resultColumns
                Values = resultValues
            }
