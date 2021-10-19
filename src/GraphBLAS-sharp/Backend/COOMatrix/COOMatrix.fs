namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.COOMatrix.Utilities
open Microsoft.FSharp.Quotations

type TupleMatrix<'elem when 'elem: struct> =
    { RowIndices: ClArray<int>
      ColumnIndices: ClArray<int>
      Values: ClArray<'elem> }

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
    
    let getTuples (clContext: ClContext) =
        
        let copy = GraphBLAS.FSharp.Backend.ClArray.copy clContext
        let copyData = GraphBLAS.FSharp.Backend.ClArray.copy clContext

        fun (processor: MailboxProcessor<_>) workGroupSize (matrix: COOMatrix<'a>) ->
                
            let resultRows = copy processor workGroupSize matrix.Rows
            let resultColumns = copy processor workGroupSize matrix.Columns
            let resultValues = copyData processor workGroupSize matrix.Values

            
            { RowIndices = resultRows
              ColumnIndices = resultColumns
              Values = resultValues }
