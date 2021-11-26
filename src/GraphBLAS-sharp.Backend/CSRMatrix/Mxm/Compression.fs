namespace GraphBLAS.FSharp.Backend.CSRMatrix.Mxm

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend

module internal rec Compression =
    let run (clContext: ClContext) workGroupSize =

        let sum =
            ClArray.prefixSumExcludeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) (plus: Expr<'a -> 'a -> 'a>) ->
            let positions = (preparePositions clContext workGroupSize) processor matrix

            let resultColumnIndicesLength = Array.zeroCreate 1

            let resultColumnIndicesLengthGpu = clContext.CreateClArray<_>(1)

            let _, r = sum processor positions resultColumnIndicesLengthGpu

            let resultColumnIndicesLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultColumnIndicesLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            (setRowPointers clContext workGroupSize) processor matrix positions

            let (resultColumnIndices, resultValues) = (setColumnsAndValues clContext workGroupSize) processor matrix positions resultColumnIndicesLength plus

            {
                RowCount = matrix.RowCount
                ColumnCount = matrix.ColumnCount
                RowPointers = matrix.RowPointers
                Columns = resultColumnIndices
                Values = resultValues
            }

    let private setColumnsAndValues (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) (positions: ClArray<int>) (resultColumnIndicesLength: int) (plus: Expr<'a -> 'a -> 'a>) ->
            let columnIndicesLength = positions.Length - 1

            let setColumnsAndValues =
                <@
                    fun (ndRange: Range1D)
                        (columnIndicesBuffer: ClArray<int>)
                        (valuesBuffer: ClArray<'a>)
                        (resultColumnIndicesBuffer: ClArray<int>)
                        (resultValuesBuffer: ClArray<'a>)
                        (positionsBuffer: ClArray<int>) ->

                        let i = ndRange.GlobalID0

                        if i = columnIndicesLength - 1
                        || i < columnIndicesLength
                        && positionsBuffer.[i] <> positionsBuffer.[i + 1]
                        then
                            let index = positionsBuffer.[i]

                            resultColumnIndicesBuffer.[index] <- columnIndicesBuffer.[i]

                            let mutable j = i - 1
                            let mutable buff = valuesBuffer.[i]
                            while j >= 0 && positionsBuffer.[j] = positionsBuffer.[j + 1] do
                                buff <- (%plus) buff valuesBuffer.[j]
                                j <- j - 1

                            resultValuesBuffer.[index] <- buff
                @>

            let resultColumnIndices =
                clContext.CreateClArray(
                    resultColumnIndicesLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let resultValues =
                clContext.CreateClArray(
                    resultColumnIndicesLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let kernel = clContext.CreateClKernel(setColumnsAndValues)

            let ndRange = Range1D.CreateValid(columnIndicesLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.SetArguments
                            ndRange
                            matrix.Columns
                            matrix.Values
                            resultColumnIndices
                            resultValues
                            positions)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultColumnIndices, resultValues

    let private setRowPointers (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) (positions: ClArray<int>) ->
            let rowPointersLength = matrix.RowPointers.Length

            let setRowPointers =
                <@
                    fun (ndRange: Range1D)
                        (rowPointersBuffer: ClArray<int>)
                        (positionsBuffer: ClArray<int>) ->

                        let i = ndRange.GlobalID0
                        if i < rowPointersLength then rowPointersBuffer.[i] <- positionsBuffer.[rowPointersBuffer.[i]]
                @>

            let kernel = clContext.CreateClKernel(setRowPointers)

            let ndRange = Range1D.CreateValid(rowPointersLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.SetArguments
                            ndRange
                            matrix.RowPointers
                            positions)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))


    let private preparePositions (clContext: ClContext) workGroupSize =
        let preparePositions =
            <@
                fun (ndRange: Range1D)
                    (rowPointersBuffer: ClArray<int>)
                    (columnsBuffer: ClArray<int>)
                    (rawPositionsBuffer: ClArray<int>) ->

                    let i = ndRange.GlobalID0
                    let localID = ndRange.LocalID0
                    let workGroupNumber = i / workGroupSize

                    let beginIndex = rowPointersBuffer.[workGroupNumber]
                    let mutable j = localID + beginIndex
                    let endIndex = rowPointersBuffer.[workGroupNumber + 1]
                    while j < endIndex do
                        let currColumn = columnsBuffer.[j]
                        if j < endIndex - 1 && currColumn = columnsBuffer.[j + 1] then rawPositionsBuffer.[j] <- 0 else rawPositionsBuffer.[j] <- 1
                        j <- j + workGroupSize
            @>

        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) ->
            let rawPositions =
                clContext.CreateClArray(
                    matrix.Columns.Length + 1,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )

            let kernel = clContext.CreateClKernel(preparePositions)

            let ndRange = Range1D(workGroupSize * matrix.RowCount)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.SetArguments
                            ndRange
                            matrix.RowPointers
                            matrix.Columns
                            rawPositions)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            rawPositions
