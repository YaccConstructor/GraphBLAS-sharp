open Expecto

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend.Common
open Brahma.FSharp.OpenCL.Core
open OpenCL.Net
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Algorithms
open GraphBLAS.FSharp.IO

[<Tests>]
let allTests =
    testList "All tests" [
        Backend.PrefixSum.tests
        Backend.BitonicSort.tests
        Backend.RemoveDuplicates.tests
        Matrix.EWiseAdd.tests
        Matrix.GetTuples.tests
        Matrix.Mxv.tests
        Matrix.Transpose.tests
        Algo.Bfs.tests
    ]
    |> testSequenced

[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv

    // graphblas {
    //     let! matrix =
    //         MtxReader("arc130.mtx").ReadMatrix(fun _ -> 1)
    //         |> Matrix.switch CSR

    //     return!
    //         BFS.levelSingleSource matrix 0
    //         >>= Vector.synchronizeAndReturn
    // }
    // |> EvalGB.withClContext (OpenCLEvaluationContext())
    // |> EvalGB.runSync
    // |> printfn "%A"

    // 0
