namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined

module internal Compression =
    let private initFlags
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (columns: ClArray<int>)
        (length: int) =
            let initFlags =
                <@
                    fun (range: Range1D)
                        (columnsBuffer: ClArray<int>)
                        (headsBuffer: ClArray<int>)
                        (tailsBuffer: ClArray<int>) ->

                        let i = range.GlobalID0

                        if i < length then
                            let column = columnsBuffer.[i]
                            if i = 0 || columnsBuffer.[i - 1] <> column then headsBuffer.[i] <- 1 else headsBuffer.[i] <- 0
                            if i = length - 1 || column <> columnsBuffer.[i + 1] then tailsBuffer.[i] <- 1 else tailsBuffer.[i] <- 0
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

    let private createFlags
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (rowPointers: ClArray<int>)
        (columns: ClArray<int>) =
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

    let run
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrix: CSRMatrix<'a>)
        (plus: Expr<'a -> 'a -> 'a>)
        (zero: 'a) =
            let rowPointersCompressed = ClArray.removeDuplications clContext workGroupSize processor matrix.RowPointers
            let heads, tails = createFlags clContext workGroupSize processor rowPointersCompressed matrix.Columns
            processor.Post(Msg.CreateFreeMsg<_>(rowPointersCompressed))

            let scannedValues = PrefixSum.byHeadFlagsInclude clContext workGroupSize processor heads matrix.Values plus zero
            processor.Post(Msg.CreateFreeMsg<_>(heads))

            let resultLengthGpu = clContext.CreateClCell()
            let positions, _ = PrefixSum.standardExcludeInplace clContext workGroupSize processor tails resultLengthGpu

            let resultLength =
                ClCell.toHost resultLengthGpu
                |> ClTask.runSync clContext
            processor.Post(Msg.CreateFreeMsg<_>(resultLengthGpu))

            let resultColumns = Scatter.run clContext workGroupSize processor positions resultLength matrix.Columns
            let resultValues = Scatter.run clContext workGroupSize processor positions resultLength scannedValues
            processor.Post(Msg.CreateFreeMsg<_>(scannedValues))
            ScatterRowPointers.runInPlace clContext workGroupSize processor positions matrix.Columns.Length resultLength matrix.RowPointers
            processor.Post(Msg.CreateFreeMsg<_>(positions))

            {
                RowCount = matrix.RowCount
                ColumnCount = matrix.ColumnCount
                RowPointers = matrix.RowPointers
                Columns = resultColumns
                Values = resultValues
            }
