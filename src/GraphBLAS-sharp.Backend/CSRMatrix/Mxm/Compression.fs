namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

module internal rec Compression =
    let run (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) (plus: Expr<'a -> 'a -> 'a>) (zero: 'a) ->

            let heads, tails = createFlags clContext workGroupSize processor matrix.RowPointers matrix.Columns.Length

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

            let resultColumns =
                clContext.CreateClArray(
                    resultLength
                )

            let resultValues =
                clContext.CreateClArray(
                    resultLength
                )

            //set positions

            {
                RowCount = matrix.RowCount
                ColumnCount = matrix.ColumnCount
                RowPointers = matrix.RowPointers
                Columns = resultColumns
                Values = resultValues
            }

    // TODO: неправильно работает, флаги должны стоять не только там, где стоят, а еще и в началах групп с одинаковыми индексами
    // TODO: необходима ли инициализация возвращаемых массивов нулями?
    // TODO: оптимальней ли будет сначала убрать повторяющиеся значения из rowPointers?
    let private createFlags (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>) (rowPointers: ClArray<int>) (length: int) ->
            let rowPointersLength = rowPointers.Length

            let createFlags =
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

            let kernel = clContext.CreateClKernel(createFlags)

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


    // let run (clContext: ClContext) workGroupSize =

    //     let sum =
    //         ClArray.prefixSumExcludeInplace clContext workGroupSize

    //     fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) (plus: Expr<'a -> 'a -> 'a>) ->
    //         let positions = (preparePositions clContext workGroupSize) processor matrix

    //         let resultColumnIndicesLength = Array.zeroCreate 1

    //         let resultColumnIndicesLengthGpu = clContext.CreateClArray<_>(1)

    //         let _, r = sum processor positions resultColumnIndicesLengthGpu

    //         let resultColumnIndicesLength =
    //             let res =
    //                 processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultColumnIndicesLength, ch))

    //             processor.Post(Msg.CreateFreeMsg<_>(r))

    //             res.[0]

    //         (setRowPointers clContext workGroupSize) processor matrix positions

    //         let (resultColumnIndices, resultValues) = (setColumnsAndValues clContext workGroupSize) processor matrix positions resultColumnIndicesLength plus

    //         {
    //             RowCount = matrix.RowCount
    //             ColumnCount = matrix.ColumnCount
    //             RowPointers = matrix.RowPointers
    //             Columns = resultColumnIndices
    //             Values = resultValues
    //         }

    // let private setColumnsAndValues (clContext: ClContext) workGroupSize =
    //     fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) (positions: ClArray<int>) (resultColumnIndicesLength: int) (plus: Expr<'a -> 'a -> 'a>) ->
    //         let columnIndicesLength = positions.Length - 1

    //         let setColumnsAndValues =
    //             <@
    //                 fun (ndRange: Range1D)
    //                     (columnIndicesBuffer: ClArray<int>)
    //                     (valuesBuffer: ClArray<'a>)
    //                     (resultColumnIndicesBuffer: ClArray<int>)
    //                     (resultValuesBuffer: ClArray<'a>)
    //                     (positionsBuffer: ClArray<int>) ->

    //                     let i = ndRange.GlobalID0

    //                     if i = columnIndicesLength - 1
    //                     || i < columnIndicesLength
    //                     && positionsBuffer.[i] <> positionsBuffer.[i + 1]
    //                     then
    //                         let index = positionsBuffer.[i]

    //                         resultColumnIndicesBuffer.[index] <- columnIndicesBuffer.[i]

    //                         let mutable j = i - 1
    //                         let mutable buff = valuesBuffer.[i]
    //                         while j >= 0 && positionsBuffer.[j] = positionsBuffer.[j + 1] do
    //                             buff <- (%plus) buff valuesBuffer.[j]
    //                             j <- j - 1

    //                         resultValuesBuffer.[index] <- buff
    //             @>

    //         let resultColumnIndices =
    //             clContext.CreateClArray(
    //                 resultColumnIndicesLength,
    //                 hostAccessMode = HostAccessMode.NotAccessible,
    //                 deviceAccessMode = DeviceAccessMode.WriteOnly
    //             )

    //         let resultValues =
    //             clContext.CreateClArray(
    //                 resultColumnIndicesLength,
    //                 hostAccessMode = HostAccessMode.NotAccessible,
    //                 deviceAccessMode = DeviceAccessMode.WriteOnly
    //             )

    //         let kernel = clContext.CreateClKernel(setColumnsAndValues)

    //         let ndRange = Range1D.CreateValid(columnIndicesLength, workGroupSize)

    //         processor.Post(
    //             Msg.MsgSetArguments
    //                 (fun () ->
    //                     kernel.ArgumentsSetter
    //                         ndRange
    //                         matrix.Columns
    //                         matrix.Values
    //                         resultColumnIndices
    //                         resultValues
    //                         positions)
    //         )

    //         processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    //         resultColumnIndices, resultValues

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
