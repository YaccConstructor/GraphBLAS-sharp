namespace GraphBLAS.FSharp.Backend.COOMatrix

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common

type internal GetTuples<'a>(matrix: COOMatrix<'a>) =
    member this.Invoke() = opencl {
        if matrix.Values.Length = 0 then
            let! resultRows = Copy.copyArray matrix.Rows
            let! resultColumns = Copy.copyArray matrix.Columns
            let! resultValues = Copy.copyArray matrix.Values

            return {
                RowIndices = resultRows
                ColumnIndices = resultColumns
                Values = resultValues
            }

        else
            return {
                RowIndices = matrix.Rows
                ColumnIndices = matrix.Columns
                Values = matrix.Values
            }
    }
