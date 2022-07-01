namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined

module internal Converter =
    let private prepareRows
        (clContext: ClContext)
        workGroupSize =

        let create = ClArray.create 0 clContext workGroupSize

        let prepareRows =
            <@
                fun (ndRange: Range1D)
                    (rowPointers: ClArray<int>)
                    (rowPointersLength: int)
                    (rows: ClArray<int>) ->

                    let i = ndRange.GlobalID0 + 1
                    if i < rowPointersLength - 1 then atomic (+) rows.[rowPointers.[i]] 1 |> ignore
            @>
        let program = clContext.Compile(prepareRows)

        fun (processor: MailboxProcessor<_>)
            (matrix: CSRMatrix<'a>) ->

            let rows = create processor matrix.Columns.Length

            let rowPointersLength = matrix.RowPointers.Length

            let kernel = program.GetKernel()

            if rowPointersLength > 2 then
                let ndRange = Range1D.CreateValid(rowPointersLength - 2, workGroupSize)
                processor.Post(
                    Msg.MsgSetArguments
                        (fun () ->
                            kernel.KernelFunc
                                ndRange
                                matrix.RowPointers
                                rowPointersLength
                                rows)
                )
                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            rows

    let toCOO
        (clContext: ClContext)
        workGroupSize =

        let prepareRows = prepareRows clContext workGroupSize
        let scanIncludeInPlace = PrefixSum.standardIncludeInplace clContext workGroupSize
        let copy = ClArray.copy clContext workGroupSize
        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (matrix: CSRMatrix<'a>) ->

            let nnz = clContext.CreateClArray(1)

            let rowIndices = prepareRows processor matrix
            scanIncludeInPlace processor rowIndices nnz
            |> ignore

            processor.Post(Msg.CreateFreeMsg(nnz))

            let colIndices =
                copy processor matrix.Columns

            let values =
                copyData processor matrix.Values

            {
                Context = clContext
                RowCount = matrix.RowCount
                ColumnCount = matrix.ColumnCount
                Rows = rowIndices
                Columns = colIndices
                Values = values
            }
