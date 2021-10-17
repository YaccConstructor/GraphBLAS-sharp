namespace GraphBLAS.FSharp.Backend.COOMatrix

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.COOMatrix.Utilities
open Microsoft.FSharp.Quotations

module EWiseAdd =

    let gpuEWiseAdd (clContext: ClContext) (opAdd: Expr<'a -> 'a -> 'a>) workGroupSize =

        let merge = Merge.merge clContext workGroupSize

        let preparePositions =
            PreparePositions.preparePositions clContext opAdd workGroupSize

        let setPositions =
            SetPositions.setPositions<'a> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: GpuCOOMatrix<'a>) (matrixRight: GpuCOOMatrix<'a>) ->

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

            { GpuCOOMatrix.RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              Rows = resultRows
              Columns = resultColumns
              Values = resultValues }
