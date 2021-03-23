namespace GraphBLAS.FSharp.Backend.COOMatrix

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp

module internal GetTuples =
    let from (matrix: COOMatrix<'a>) = opencl {
        return {
            RowIndices = matrix.Rows
            ColumnIndices = matrix.Columns
            Values = matrix.Values
        }
    }
