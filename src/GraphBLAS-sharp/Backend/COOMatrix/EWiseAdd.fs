namespace GraphBLAS.FSharp.Backend.COOMatrix

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.COOMatrix.Utilities

module internal EWiseAdd =
    let private runNonEmpty (matrixLeft: COOMatrix<'a>) (matrixRight: COOMatrix<'a>) (mask: Mask2D option) (monoid: IMonoid<'a>) = opencl {
        let! allRows, allColumns, allValues = merge matrixLeft matrixRight mask

        let (ClosedBinaryOp plus) = monoid.Plus
        let! rawPositions = preparePositions allRows allColumns allValues plus
        let! resultRows, resultColumns, resultValues = setPositions allRows allColumns allValues rawPositions

        return {
            RowCount = matrixLeft.RowCount
            ColumnCount = matrixLeft.ColumnCount
            Rows = resultRows
            Columns = resultColumns
            Values = resultValues
        }
    }

    let run (matrixLeft: COOMatrix<'a>) (matrixRight: COOMatrix<'a>) (mask: Mask2D option) (monoid: IMonoid<'a>) = opencl {
        if matrixLeft.Values.Length = 0 then
            let! resultRows = Copy.copyArray matrixRight.Rows
            let! resultColumns = Copy.copyArray matrixRight.Columns
            let! resultValues = Copy.copyArray matrixRight.Values

            return {
                RowCount = matrixRight.RowCount
                ColumnCount = matrixRight.ColumnCount
                Rows = resultRows
                Columns = resultColumns
                Values = resultValues
            }

        elif matrixRight.Values.Length = 0 then
            let! resultRows = Copy.copyArray matrixLeft.Rows
            let! resultColumns = Copy.copyArray matrixLeft.Columns
            let! resultValues = Copy.copyArray matrixLeft.Values

            return {
                RowCount = matrixLeft.RowCount
                ColumnCount = matrixLeft.ColumnCount
                Rows = resultRows
                Columns = resultColumns
                Values = resultValues
            }

        else
            return! runNonEmpty matrixLeft matrixRight mask monoid
    }
