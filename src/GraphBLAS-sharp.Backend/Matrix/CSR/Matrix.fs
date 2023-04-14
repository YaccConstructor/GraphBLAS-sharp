namespace GraphBLAS.FSharp.Backend.Matrix.CSR

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClVector
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClCell


module Matrix =
    let toCOO (clContext: ClContext) workGroupSize =
        let prepare =
            Common.expandRowPointers clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            let rows =
                prepare processor allocationMode matrix.RowPointers matrix.Columns.Length matrix.RowCount

            let cols =
                copy processor allocationMode matrix.Columns

            let values =
                copyData processor allocationMode matrix.Values

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = cols
              Values = values }

    let toCOOInplace (clContext: ClContext) workGroupSize =
        let prepare =
            Common.expandRowPointers clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            let rows =
                prepare processor allocationMode matrix.RowPointers matrix.Columns.Length matrix.RowCount

            processor.Post(Msg.CreateFreeMsg(matrix.RowPointers))

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              Rows = rows
              Columns = matrix.Columns
              Values = matrix.Values }

    let map = CSR.Map.run

    let map2 = Map2.run

    let map2AtLeastOneToCOO<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        Map2AtLeastOne.runToCOO clContext (Convert.atLeastOneToOption opAdd) workGroupSize

    let map2AtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        Map2AtLeastOne.run clContext (Convert.atLeastOneToOption opAdd) workGroupSize

    let transposeInplace (clContext: ClContext) workGroupSize =

        let toCOOInplace = toCOOInplace clContext workGroupSize

        let transposeInplace =
            COO.Matrix.transposeInplace clContext workGroupSize

        let toCSRInplace =
            COO.Matrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            toCOOInplace queue allocationMode matrix
            |> transposeInplace queue
            |> toCSRInplace queue allocationMode

    let transpose (clContext: ClContext) workGroupSize =

        let toCOO = toCOO clContext workGroupSize

        let transposeInplace =
            COO.Matrix.transposeInplace clContext workGroupSize

        let toCSRInplace =
            COO.Matrix.toCSRInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            toCOO queue allocationMode matrix
            |> transposeInplace queue
            |> toCSRInplace queue allocationMode

    let byRowsLazy (clContext: ClContext) workGroupSize =

        let getChunkValues = ClArray.getChunk clContext workGroupSize

        let getChunkIndices = ClArray.getChunk clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->

            let getChunkValues =
                getChunkValues processor allocationMode matrix.Values

            let getChunkIndices =
                getChunkIndices processor allocationMode matrix.Columns

            let creatSparseVector values columns =
                { Context = clContext
                  Indices = columns
                  Values = values
                  Size = matrix.ColumnCount }

            matrix.RowPointers.ToHost processor
            |> Seq.pairwise
            |> Seq.map
                (fun (first, second) ->
                    lazy
                        (if second - first > 0 then
                             let values = getChunkValues first second
                             let columns = getChunkIndices first second

                             Some <| creatSparseVector values columns
                         else
                             None))

    let byRows (clContext: ClContext) workGroupSize =

        let runLazy = byRowsLazy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) ->
            runLazy processor allocationMode matrix
            |> Seq.map (fun lazyValue -> lazyValue.Value)
            |> Seq.toArray

    module SpGeMM =
        let masked
            (clContext: ClContext)
            workGroupSize
            (opAdd: Expr<'c -> 'c -> 'c option>)
            (opMul: Expr<'a -> 'b -> 'c option>)
            =

            let run =
                SpGeMM.Masked.run clContext workGroupSize opAdd opMul

            fun (queue: MailboxProcessor<_>) (matrixLeft: ClMatrix.CSR<'a>) (matrixRight: ClMatrix.CSC<'b>) (mask: ClMatrix.COO<_>) ->

                run queue matrixLeft matrixRight mask

        let expand
            (clContext: ClContext)
            workGroupSize
            (opAdd: Expr<'c -> 'c -> 'c option>)
            (opMul: Expr<'a -> 'b -> 'c option>)
            =

            let run =
                SpGeMM.Expand.run clContext workGroupSize opAdd opMul

            fun (queue: MailboxProcessor<_>) allocationMode (leftMatrix: ClMatrix.CSR<'a>) (rightMatrix: ClMatrix.CSR<'b>) ->

                let values, columns, rows =
                    run queue allocationMode leftMatrix rightMatrix

                { COO.Context = clContext
                  ColumnCount = rightMatrix.ColumnCount
                  RowCount = leftMatrix.RowCount
                  Values = values
                  Columns = columns
                  Rows = rows }
