namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined

module internal rec Converter =
    let toCOO
        (clContext: ClContext)
        workGroupSize =

        let prepareRows = prepareRows clContext workGroupSize
        let scanIncludeInPlace = PrefixSum.standardIncludeInplace clContext workGroupSize
        let copy = ClArray.copy clContext workGroupSize
        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrix: CSRMatrix<'a>) ->

            let nnz = clContext.CreateClCell()

            let rowIndices = prepareRows processor matrix
            scanIncludeInPlace processor rowIndices nnz
            |> ignore

            processor.Post(Msg.CreateFreeMsg(nnz))

            let colIndices =
                copy processor matrix.Columns

            let values =
                copyData processor matrix.Values

            {
                RowCount = matrix.RowCount
                ColumnCount = matrix.ColumnCount
                Rows = rowIndices
                Columns = colIndices
                Values = values
            }

    let private prepareRows
        (clContext: ClContext)
        workGroupSize =

        let create = ClArray.create clContext workGroupSize

        let prepareRows =
            <@
                fun (ndRange: Range1D)
                    (rowPointers: ClArray<int>)
                    (rowPointersLength: int)
                    (rows: ClArray<int>) ->

                    let i = ndRange.GlobalID0 + 1
                    if i < rowPointersLength - 1 then atomic (+) rows.[rowPointers.[i]] 1 |> ignore
            @>
        let kernel = clContext.CreateClKernel(prepareRows)

        fun (processor: MailboxProcessor<_>)
            (matrix: CSRMatrix<'a>) ->

            let rows = create processor matrix.Columns.Length 0

            let rowPointersLength = matrix.RowPointers.Length

            if rowPointersLength > 2 then
                let ndRange = Range1D.CreateValid(rowPointersLength - 2, workGroupSize)
                processor.Post(
                    Msg.MsgSetArguments
                        (fun () ->
                            kernel.ArgumentsSetter
                                ndRange
                                matrix.RowPointers
                                rowPointersLength
                                rows)
                )
                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            rows
