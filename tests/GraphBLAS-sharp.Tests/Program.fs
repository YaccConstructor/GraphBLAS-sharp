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
        // Backend.PrefixSum.tests
        // Backend.BitonicSort.tests
        // Backend.RemoveDuplicates.tests
        // Matrix.GetTuples.tests
        // Matrix.Mxv.tests
        // Matrix.EWiseAdd.tests
        Matrix.Transpose.tests
    ]
    |> testSequenced

[<EntryPoint>]
let main argv =
    // opencl {
    //     let array = Array.init 4 (fun i -> 4 - i)
    //     do! BitonicSort.sortInplace3 array
    //     let! _ = ToHost array
    //     return array
    // }
    // |> OpenCLEvaluationContext().RunSync
    // |> printfn "\n%A"
    // translate2opencl (OpenCLEvaluationContext().Provider) kernel
    // |> printfn "%A"
    // 0
    allTests
    |> runTestsWithCLIArgs [] argv
