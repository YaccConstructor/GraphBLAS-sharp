namespace rec GraphBLAS.FSharp.Backend.CSRMatrix

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open Brahma.OpenCL

type internal SpMSpV<'a>(matrix: CSRMatrix<'a>, vector: COOVector<'a>, semiring: ISemiring<'a>) =
    member this.Invoke() = opencl { () }

module private SpMSpV =
    let a = 1
