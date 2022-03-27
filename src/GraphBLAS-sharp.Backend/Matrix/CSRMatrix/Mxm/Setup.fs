namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined

module internal Setup =
    let private getRowLengths
        (clContext: ClContext)
        workGroupSize =

        let get =
            <@
                fun (ndRange: Range1D)
                    (firstRowPointers: ClArray<int>)
                    (size: int)
                    (rowLengths: ClArray<int>)
                    (resultRowLengths: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        let index = firstRowPointers.[i + 1]
                        if i < size - 1 && firstRowPointers.[i] <> index then
                            resultRowLengths.[i] <- rowLengths.[index - 1]
                        else
                            resultRowLengths.[i] <- 0
            @>
        let kernel = clContext.CreateClProgram(get).GetKernel()

        fun (processor: MailboxProcessor<_>)
            (rowLengths: ClArray<int>)
            (matrixLeft: CSRMatrix<'a>) ->

            let resultRowLengths =
                clContext.CreateClArray(
                    matrixLeft.RowPointers.Length,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let size = matrixLeft.RowPointers.Length

            let ndRange = Range1D.CreateValid(size, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            matrixLeft.RowPointers
                            size
                            rowLengths
                            resultRowLengths)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultRowLengths

    let private initRowLengths
        (clContext: ClContext)
        workGroupSize =

        let init =
            <@
                fun (ndRange: Range1D)
                    (secondRowPointers: ClArray<int>)
                    (firstColumns: ClArray<int>)
                    (size: int)
                    (lenghts: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < size then
                        let column = firstColumns.[i]
                        lenghts.[i] <- secondRowPointers.[column + 1] - secondRowPointers.[column]
            @>
        let kernel = clContext.CreateClProgram(init).GetKernel()

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>) ->

            let rowLengths =
                clContext.CreateClArray(
                    matrixLeft.Columns.Length,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let size = rowLengths.Length

            let ndRange = Range1D.CreateValid(size, workGroupSize)
            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            matrixRight.RowPointers
                            matrixLeft.Columns
                            size
                            rowLengths)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            rowLengths

    let run
        (clContext: ClContext)
        workGroupSize =

        let initRowLengths = initRowLengths clContext workGroupSize
        let create = ClArray.create 0 clContext workGroupSize
        let scatter = Scatter.constInPlace 1 clContext workGroupSize
        let scanByHeadFlagsInclude = PrefixSum.byHeadFlagsInclude <@ (+) @> clContext workGroupSize
        let scanExcludeInPlace = PrefixSum.standardExcludeInplace clContext workGroupSize
        let getRowLengths = getRowLengths clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrixLeft: CSRMatrix<'a>)
            (matrixRight: CSRMatrix<'a>) ->

            let rowLengths = initRowLengths processor matrixLeft matrixRight

            let headFlags = create processor matrixLeft.Columns.Length

            scatter processor matrixLeft.RowPointers headFlags

            let rowLengths' = scanByHeadFlagsInclude processor headFlags rowLengths 0
            processor.Post(Msg.CreateFreeMsg<_>(rowLengths))

            let resultRowPointers = getRowLengths processor rowLengths' matrixLeft
            processor.Post(Msg.CreateFreeMsg<_>(rowLengths'))

            let resultNNZGpu = clContext.CreateClCell()
            scanExcludeInPlace processor resultRowPointers resultNNZGpu
            |> ignore

            let resultNNZ =
                ClCell.toHost resultNNZGpu
                |> ClTask.runSync clContext
            processor.Post(Msg.CreateFreeMsg<_>(resultNNZGpu))

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
                Context = clContext
                RowCount = matrixLeft.RowCount
                ColumnCount = matrixRight.ColumnCount
                RowPointers = resultRowPointers
                Columns = resultColumns
                Values = resultValues
            },
            headFlags
