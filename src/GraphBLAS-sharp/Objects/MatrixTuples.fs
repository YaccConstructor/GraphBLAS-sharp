namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

type MatrixTuples<'a> =
    {
        RowIndices: int[]
        ColumnIndices: int[]
        Values: 'a[]
    }

// ждём тайпклассов чтобы можно было вызывать synchronize для всех объектов,
// для которых он реализован, не привязывая реализацию к классу (как стратегия)
module MatrixTuples =
    let synchronize (matrixTuples: MatrixTuples<'a>) =
        opencl {
            let! _ = ToHost matrixTuples.RowIndices
            let! _ = ToHost matrixTuples.ColumnIndices
            let! _ = ToHost matrixTuples.Values
            return ()
        }
        |> EvalGB.fromCl
