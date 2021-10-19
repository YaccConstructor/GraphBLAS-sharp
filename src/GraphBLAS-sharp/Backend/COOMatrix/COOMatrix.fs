namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.COOMatrix.Utilities
open Microsoft.FSharp.Quotations


type COOMatrix<'elem when 'elem: struct> =
    { RowCount: int
      ColumnCount: int
      Rows: ClArray<int>
      Columns: ClArray<int>
      Values: ClArray<'elem> }

module COOMatrix =
    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a -> 'a -> 'a>) workGroupSize =

        let merge = Merge.merge clContext workGroupSize

        let preparePositions =
            PreparePositions.preparePositions clContext opAdd workGroupSize

        let setPositions =
            SetPositions.setPositions<'a> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: COOMatrix<'a>) (matrixRight: COOMatrix<'a>) ->

            let allRows, allColumns, allValues =
                merge
                    queue
                    matrixLeft.Rows
                    matrixLeft.Columns
                    matrixLeft.Values
                    matrixRight.Rows
                    matrixRight.Columns
                    matrixRight.Values


            use rawPositions =
                preparePositions queue allRows allColumns allValues

            let resultRows, resultColumns, resultValues, resultLength =
                setPositions queue allRows allColumns allValues rawPositions

            queue.Post(Msg.CreateFreeMsg<_>(allRows))
            queue.Post(Msg.CreateFreeMsg<_>(allColumns))
            queue.Post(Msg.CreateFreeMsg<_>(allValues))

            { RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              Rows = resultRows
              Columns = resultColumns
              Values = resultValues }
