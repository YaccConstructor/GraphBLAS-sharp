namespace GraphBLAS.FSharp.Backend.COOMatrix

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common

module internal EWiseAdd =
    let cooNotEmpty (matrixLeft: COOFormat<'a>) (matrixRight: COOFormat<'a>) (mask: Mask2D option) (semiring: Semiring<'a>) : OpenCLEvaluation<COOFormat<'a>> = opencl {
        let! allRows, allColumns, allValues = Merge.runForMatrix matrixLeft matrixRight mask

        let (BinaryOp append) = semiring.PlusMonoid.Append
        let! rawPositions = PreparePositions.runForMatrix allRows allColumns allValues append

        let! resultRows, resultColumns, resultValues = SetPositions.runForMatrix allRows allColumns allValues rawPositions

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

    let sparseNotEmpty (leftIndices: int[]) (leftValues: 'a[]) (rightIndices: int[]) (rightValues: 'a[]) (mask: Mask1D option) (semiring: Semiring<'a>) : OpenCLEvaluation<int[] * 'a[]> = opencl {
        let! allIndices, allValues = Merge.runForVector leftIndices leftValues rightIndices rightValues mask

        let (BinaryOp append) = semiring.PlusMonoid.Append
        let! rawPositions = PreparePositions.runForVector allIndices allValues append

        return! SetPositions.runForVector allIndices allValues rawPositions
    }

    let sparse (leftIndices: int[]) (leftValues: 'a[]) (rightIndices: int[]) (rightValues: 'a[]) (mask: Mask1D option) (semiring: Semiring<'a>) : OpenCLEvaluation<int[] * 'a[]> =
        if leftValues.Length = 0 then
            opencl {
                let! resultIndices = Copy.run rightIndices
                let! resultValues = Copy.run rightValues

                return resultIndices, resultValues
            }
        elif rightIndices.Length = 0 then
            opencl {
                let! resultIndices = Copy.run leftIndices
                let! resultValues = Copy.run leftValues

                return resultIndices, resultValues
            }
        else sparseNotEmpty leftIndices leftValues rightIndices rightValues mask semiring
