namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined

module internal Compression =
    let private initFlags
        (clContext: ClContext)
        workGroupSize =

        let initFlags =
            <@
                fun (range: Range1D)
                    (columnsBuffer: ClArray<int>)
                    (headsBuffer: ClArray<int>)
                    (tailsBuffer: ClArray<int>)
                    (length: int) ->

                    let i = range.GlobalID0

                    if i < length then
                        let column = columnsBuffer.[i]
                        if i = 0 || columnsBuffer.[i - 1] <> column then headsBuffer.[i] <- 1 else headsBuffer.[i] <- 0
                        if i = length - 1 || column <> columnsBuffer.[i + 1] then tailsBuffer.[i] <- 1 else tailsBuffer.[i] <- 0
            @>
        let kernel = clContext.CreateClKernel(initFlags)

        fun (processor: MailboxProcessor<_>)
            (columns: ClArray<int>)
            (length: int) ->

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

                let ndRange = Range1D.CreateValid(length, workGroupSize)
                processor.Post(
                    Msg.MsgSetArguments
                        (fun () ->
                            kernel.ArgumentsSetter
                                ndRange
                                columns
                                heads
                                tails
                                length)
                )
                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

                heads, tails

    let private createFlags
        (clContext: ClContext)
        workGroupSize =

        let initFlags = initFlags clContext workGroupSize

        let updateFlags =
            <@
                fun (range: Range1D)
                    (rowPointersBuffer: ClArray<int>)
                    (rowPointersLength: int)
                    (headsBuffer: ClArray<int>)
                    (tailsBuffer: ClArray<int>)
                    (length: int) ->

                    let i = range.GlobalID0

                    if i < rowPointersLength then
                        let j = rowPointersBuffer.[i]
                        if j < length then headsBuffer.[j] <- 1
                        if j > 0 then tailsBuffer.[j - 1] <- 1
            @>
        let kernel = clContext.CreateClKernel(updateFlags)

        fun (processor: MailboxProcessor<_>)
            (rowPointers: ClArray<int>)
            (columns: ClArray<int>) ->
                let length = columns.Length

                let heads, tails = initFlags processor columns length

                let rowPointersLength = rowPointers.Length

                let ndRange = Range1D.CreateValid(rowPointersLength, workGroupSize)
                processor.Post(
                    Msg.MsgSetArguments
                        (fun () ->
                            kernel.ArgumentsSetter
                                ndRange
                                rowPointers
                                rowPointersLength
                                heads
                                tails
                                length)
                )
                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

                heads, tails

    let run
        (plus: Expr<'a -> 'a -> 'a>)
        (clContext: ClContext)
        workGroupSize =

        let removeDuplications = ClArray.removeDuplications clContext workGroupSize
        let createFlags = createFlags clContext workGroupSize
        let scanByHeadFlagsInclude = PrefixSum.byHeadFlagsInclude plus clContext workGroupSize
        let scanExcludeInPlace = PrefixSum.standardExcludeInplace clContext workGroupSize
        let scatter = Scatter.run clContext workGroupSize
        let scatterData = Scatter.run clContext workGroupSize
        let scatterRowPointers = ScatterRowPointers.runInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrix: CSRMatrix<'a>)
            (zero: 'a) ->
                let rowPointersCompressed = removeDuplications processor matrix.RowPointers
                let heads, tails = createFlags processor rowPointersCompressed matrix.Columns
                processor.Post(Msg.CreateFreeMsg<_>(rowPointersCompressed))

                let scannedValues = scanByHeadFlagsInclude processor heads matrix.Values zero
                processor.Post(Msg.CreateFreeMsg<_>(heads))

                let resultLengthGpu = clContext.CreateClCell()
                let positions, _ = scanExcludeInPlace processor tails resultLengthGpu

                let resultLength =
                    ClCell.toHost resultLengthGpu
                    |> ClTask.runSync clContext
                processor.Post(Msg.CreateFreeMsg<_>(resultLengthGpu))

                let resultColumns = scatter processor positions resultLength matrix.Columns
                let resultValues = scatterData processor positions resultLength scannedValues
                processor.Post(Msg.CreateFreeMsg<_>(scannedValues))
                scatterRowPointers processor positions matrix.Columns.Length resultLength matrix.RowPointers
                processor.Post(Msg.CreateFreeMsg<_>(positions))

                {
                    RowCount = matrix.RowCount
                    ColumnCount = matrix.ColumnCount
                    RowPointers = matrix.RowPointers
                    Columns = resultColumns
                    Values = resultValues
                }
