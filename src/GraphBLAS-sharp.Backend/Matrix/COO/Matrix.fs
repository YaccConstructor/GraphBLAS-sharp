namespace GraphBLAS.FSharp.Backend.Matrix.COO

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

module Matrix =
    let map = Map.run

    let map2 = Map2.run

    ///<param name="clContext">.</param>
    ///<param name="opAdd">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
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

        let scatter =
            Scatter.initFirsOccurrence Map.id clContext workGroupSize

        let create = ClArray.create clContext workGroupSize

        let scan =
            PrefixSum.runBackwardsIncludeInPlace <@ min @> System.Int32.MaxValue clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (rowIndices: ClArray<int>) rowCount ->

            let nnz = rowIndices.Length

            let rowPointers =
                create processor allocationMode (rowCount + 1) nnz

            scatter processor rowIndices rowPointers

            (scan processor rowPointers).Free processor

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
            Sort.Bitonic.sortKeyValuesInplace clContext workGroupSize

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
