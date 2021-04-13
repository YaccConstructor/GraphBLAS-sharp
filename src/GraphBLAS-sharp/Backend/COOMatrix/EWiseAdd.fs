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

    let run (matrixLeft: COOMatrix<'a>) (matrixRight: COOMatrix<'a>) (mask: Mask2D option) (monoid: IMonoid<'a>) =
        if matrixLeft.Values.Length = 0 then
            opencl {
                let! resultRows = Copy.run matrixRight.Rows
                let! resultColumns = Copy.run matrixRight.Columns
                let! resultValues = Copy.run matrixRight.Values

                return {
                    RowCount = matrixRight.RowCount
                    ColumnCount = matrixRight.ColumnCount
                    Rows = resultRows
                    Columns = resultColumns
                    Values = resultValues
                }
            }

        elif matrixRight.Values.Length = 0 then
            opencl {
                let! resultRows = Copy.run matrixLeft.Rows
                let! resultColumns = Copy.run matrixLeft.Columns
                let! resultValues = Copy.run matrixLeft.Values

                return {
                    RowCount = matrixLeft.RowCount
                    ColumnCount = matrixLeft.ColumnCount
                    Rows = resultRows
                    Columns = resultColumns
                    Values = resultValues
                }
            }

        else
            runNonEmpty matrixLeft matrixRight mask monoid
