namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

module internal rec Converter =
    let toCOO (clContext: ClContext) =
        let copy = ClArray.copy clContext
        let copyData = ClArray.copy clContext

        fun (processor: MailboxProcessor<_>) workGroupSize (matrix: CSRMatrix<'a>) ->

            let nnz = clContext.CreateClArray(1)

            let rowIndices = prepareRows clContext workGroupSize processor matrix
            PrefixSum.runExcludeInplace clContext workGroupSize processor rowIndices nnz 0 <@ (+) @> 0
            |> ignore

            processor.Post(Msg.CreateFreeMsg(nnz))

            let colIndices =
                copy processor workGroupSize matrix.Columns

            let values =
                copyData processor workGroupSize matrix.Values

            { RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rowIndices
              Columns = colIndices
              Values = values }

    // TODO: инициализация rows нулями
    let private prepareRows
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (matrix: CSRMatrix<'a>) =

        let rows =
            clContext.CreateClArray(
                matrix.Columns.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let rowPointersLength = matrix.RowPointers.Length

        let prepareRows =
            <@
                fun (ndRange: Range1D)
                    (rowPointers: ClArray<int>)
                    (rows: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < rowPointersLength then
                        rows.[rowPointers.[i]] <- 1
            @>

        let kernel = clContext.CreateClKernel(prepareRows)

        let ndRange = Range1D.CreateValid(rowPointersLength, workGroupSize)

        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        matrix.RowPointers
                        rows)
        )

        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        rows
