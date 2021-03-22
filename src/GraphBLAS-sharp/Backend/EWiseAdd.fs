namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common

module internal EWiseAdd =
    let cooNotEmpty (matrixLeft: COOFormat<'a>) (matrixRight: COOFormat<'a>) (mask: Mask2D option) (semiring: Semiring<'a>) : OpenCLEvaluation<COOFormat<'a>> = opencl {
        let! allRows, allColumns, allValues = Merge.run matrixLeft matrixRight mask

        let (BinaryOp append) = semiring.PlusMonoid.Append
        let! rawPositions = PreparePositions.run allRows allColumns allValues append

        let! resultRows, resultColumns, resultValues = SetPositions.run allRows allColumns allValues rawPositions

        return {
            RowCount = matrixLeft.RowCount
            ColumnCount = matrixLeft.ColumnCount
            Rows = resultRows
            Columns = resultColumns
            Values = resultValues
        }
    }

    let coo (matrixLeft: COOFormat<'a>) (matrixRight: COOFormat<'a>) (mask: Mask2D option) (semiring: Semiring<'a>) : OpenCLEvaluation<COOFormat<'a>> =
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
        else cooNotEmpty matrixLeft matrixRight mask semiring
