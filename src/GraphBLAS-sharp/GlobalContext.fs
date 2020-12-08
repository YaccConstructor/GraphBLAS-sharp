namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<AutoOpen>]
module GlobalContext =
    type MatrixBackendFormat =
        | CSR
        | COO
        | Dense

    let mutable oclContext = OpenCLEvaluationContext()
    let mutable matrixBackendFormat = CSR
