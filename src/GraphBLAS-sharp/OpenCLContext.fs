namespace GraphBLAS.FSharp

open Brahma.OpenCL
open OpenCL.Net
open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

module OpenCLContext =
    let currentContext = OpenCLEvaluationContext()
