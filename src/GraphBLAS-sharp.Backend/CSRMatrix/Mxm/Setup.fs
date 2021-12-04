namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Predefined

module internal rec Setup =
    let run (clContext: ClContext) workGroupSize =

        let sum =
            ClArray.prefixSumExcludeInplace clContext workGroupSize

        let resultNNZ = [| 0 |]

        fun (processor: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'a>) ->

            let rowLengths = initRowLengths clContext workGroupSize processor matrixLeft matrixRight
            let headFlags = getHeadFlags clContext workGroupSize processor matrixLeft
            let rowLengths' = ByHeadFlags.runInclude clContext workGroupSize processor headFlags rowLengths <@ (+) @> 0
            let resultRowPointers = getRowLengths clContext workGroupSize processor rowLengths' matrixLeft

            let resultNNZGpu = clContext.CreateClArray<_>(1)

            let _, r = sum processor resultRowPointers resultNNZGpu

            let resultNNZ =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultNNZ, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            let resultColumns =
                clContext.CreateClArray(
                    resultNNZ,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let resultValues =
                clContext.CreateClArray(
                    resultNNZ,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            {
                RowCount = matrixLeft.RowCount
                ColumnCount = matrixRight.ColumnCount
                RowPointers = resultRowPointers
                Columns = resultColumns
                Values = resultValues
            }

    let private getRowLengths
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (rowLengths: ClArray<int>)
        (matrixLeft: CSRMatrix<'a>) =

        let resultRowLengths =
            clContext.CreateClArray(
                matrixLeft.RowPointers.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let size = matrixLeft.RowCount

        let get =
            <@
                fun (ndRange: Range1D)
                    (firstRowPointers: ClArray<int>)
                    (rowLengths: ClArray<int>)
                    (resultRowLengths: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        let index = firstRowPointers.[i + 1]
                        if firstRowPointers.[i] <> index then
                            resultRowLengths.[i] <- rowLengths.[index - 1]
                        else
                            resultRowLengths.[i] <- 0
            @>

        let kernel = clContext.CreateClKernel(get)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixLeft.RowPointers
                        rowLengths
                        resultRowLengths)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        resultRowLengths

    let private initRowLengths
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>)
        (matrixRight: CSRMatrix<'a>) =

        let rowLengths =
            clContext.CreateClArray(
                matrixLeft.Columns.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let size = rowLengths.Length

        let init =
            <@
                fun (ndRange: Range1D)
                    (secondRowPointers: ClArray<int>)
                    (firstColumns: ClArray<int>)
                    (lenghts: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        lenghts.[i] <- secondRowPointers.[firstColumns.[i] + 1] - secondRowPointers.[firstColumns.[i]]
            @>

        let kernel = clContext.CreateClKernel(init)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixRight.RowPointers
                        matrixLeft.Columns
                        rowLengths)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        rowLengths

    let private getHeadFlags
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrixLeft: CSRMatrix<'a>) =

        let headFlags = ClArray.create clContext workGroupSize processor matrixLeft.Columns.Length 0

        let size = matrixLeft.RowCount

        let init =
            <@
                fun (ndRange: Range1D)
                    (rowPointers: ClArray<int>)
                    (heads: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        heads.[rowPointers.[i]] <- 1
            @>

        let kernel = clContext.CreateClKernel(init)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrixLeft.RowPointers
                        headFlags)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        headFlags
