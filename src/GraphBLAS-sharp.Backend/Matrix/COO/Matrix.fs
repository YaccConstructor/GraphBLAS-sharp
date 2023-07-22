namespace GraphBLAS.FSharp.Backend.Matrix.COO

open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClMatrix
open GraphBLAS.FSharp.Objects.ClCellExtensions
open GraphBLAS.FSharp.Objects.ArraysExtensions

module Matrix =
    let map = Map.run

    let map2 = Map2.run

    let rec map2AtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        Map2.AtLeastOne.run clContext (Convert.atLeastOneToOption opAdd) workGroupSize

    let getTuples (clContext: ClContext) workGroupSize =

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.COO<'a>) ->

            let resultRows =
                copy processor allocationMode matrix.Rows

            let resultColumns =
                copy processor allocationMode matrix.Columns

            let resultValues =
                copyData processor allocationMode matrix.Values

            { Context = clContext
              RowIndices = resultRows
              ColumnIndices = resultColumns
              Values = resultValues }

    let private compressRows (clContext: ClContext) workGroupSize =

        let compressRows =
            <@ fun (ndRange: Range1D) (rows: ClArray<int>) (nnz: int) (rowPointers: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < nnz then
                    let row = rows.[i]

                    if i = 0 || row <> rows.[i - 1] then
                        rowPointers.[row] <- i @>

        let program = clContext.Compile(compressRows)

        let create = ClArray.create clContext workGroupSize

        let scan =
            Common.PrefixSum.runBackwardsIncludeInPlace <@ min @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (rowIndices: ClArray<int>) rowCount ->

            let nnz = rowIndices.Length

            let rowPointers =
                create processor allocationMode (rowCount + 1) nnz

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(nnz, workGroupSize)
            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange rowIndices nnz rowPointers))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            (scan processor rowPointers nnz).Free processor

            rowPointers

    let toCSR (clContext: ClContext) workGroupSize =
        let prepare = compressRows clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.COO<'a>) ->
            let rowPointers =
                prepare processor allocationMode matrix.Rows matrix.RowCount

            let cols =
                copy processor allocationMode matrix.Columns

            let values =
                copyData processor allocationMode matrix.Values

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = rowPointers
              Columns = cols
              Values = values }

    let toCSRInPlace (clContext: ClContext) workGroupSize =
        let prepare = compressRows clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.COO<'a>) ->
            let rowPointers =
                prepare processor allocationMode matrix.Rows matrix.RowCount

            matrix.Rows.Free processor

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = rowPointers
              Columns = matrix.Columns
              Values = matrix.Values }

    let transposeInPlace (clContext: ClContext) workGroupSize =

        let sort =
            Common.Bitonic.sortKeyValuesInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrix: ClMatrix.COO<'a>) ->
            sort queue matrix.Columns matrix.Rows matrix.Values

            { Context = clContext
              RowCount = matrix.ColumnCount
              ColumnCount = matrix.RowCount
              Rows = matrix.Columns
              Columns = matrix.Rows
              Values = matrix.Values }

    let transpose (clContext: ClContext) workGroupSize =

        let transposeInPlace = transposeInPlace clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.COO<'a>) ->

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = copy queue allocationMode matrix.Rows
              Columns = copy queue allocationMode matrix.Columns
              Values = copyData queue allocationMode matrix.Values }
            |> transposeInPlace queue
