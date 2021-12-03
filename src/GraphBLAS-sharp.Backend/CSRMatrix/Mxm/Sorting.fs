namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

module internal rec Sorting =
    let runInPlace (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) ->
            let matrixCOO = Converter.toCOO clContext processor workGroupSize matrix

            let packedIndices = pack clContext workGroupSize processor matrixCOO.Rows matrixCOO.Columns

            RadixSort.sortByKeyInPlace clContext workGroupSize processor packedIndices matrix.Values 2

            let _, sortedColumns = unpack clContext workGroupSize processor packedIndices

            { RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = matrix.RowPointers
              Columns = sortedColumns
              Values = matrix.Values }

    let private pack
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (firstArray: ClArray<int>)
        (secondArray: ClArray<int>) =

        let length = firstArray.Length

        let pack =
            <@
                fun (ndRange: Range1D)
                    (firstArray: ClArray<int>)
                    (secondArray: ClArray<int>)
                    (outputArray: ClArray<uint64>) ->

                    let i = ndRange.GlobalID0

                    if i < length then
                        outputArray.[i] <- ((uint64 firstArray.[i]) <<< 32) ||| (uint64 secondArray.[i])
            @>

        let outputArray =
            clContext.CreateClArray(
                length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let kernel = clContext.CreateClKernel(pack)

        let ndRange = Range1D.CreateValid(length, workGroupSize)

        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        firstArray
                        secondArray
                        outputArray)
        )

        processor.Post(Msg.CreateRunMsg<_, _> kernel)

        outputArray

    let private unpack
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (inputArray: ClArray<uint64>) =

        let length = inputArray.Length

        let pack =
            <@
                fun (ndRange: Range1D)
                    (inputArray: ClArray<uint64>)
                    (firstArray: ClArray<int>)
                    (secondArray: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < length then
                        let n = inputArray.[i]
                        firstArray.[i] <- int (n >>> 32)
                        secondArray.[i] <- int n
            @>

        let firstArray =
            clContext.CreateClArray(
                length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let secondArray =
            clContext.CreateClArray(
                length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let kernel = clContext.CreateClKernel(pack)

        let ndRange = Range1D.CreateValid(length, workGroupSize)

        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        inputArray
                        firstArray
                        secondArray)
        )

        processor.Post(Msg.CreateRunMsg<_, _> kernel)

        firstArray, secondArray
