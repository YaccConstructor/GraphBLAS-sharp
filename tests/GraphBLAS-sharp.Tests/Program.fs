open Expecto

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend.Common
open Brahma.FSharp.OpenCL.Core
open OpenCL.Net

[<Tests>]
let allTests =
    testList "All tests" [
        Backend.PrefixSum.tests
        Backend.BitonicSort.tests
        Backend.RemoveDuplicates.tests
        Matrix.GetTuples.tests
        Matrix.Mxv.tests
        Matrix.Transpose.tests
    ]
    |> testSequenced

[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv
