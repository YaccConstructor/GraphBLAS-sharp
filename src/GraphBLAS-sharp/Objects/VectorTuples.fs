namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

type VectorTuples<'a> =
    {
        Indices: int[]
        Values: 'a[]
    }

module VectorTuples =
    let synchronize (vectorTuples: VectorTuples<'a>) =
        opencl {
            let! _ = ToHost vectorTuples.Indices
            let! _ = ToHost vectorTuples.Values
            return ()
        }
        |> EvalGB.fromCl
