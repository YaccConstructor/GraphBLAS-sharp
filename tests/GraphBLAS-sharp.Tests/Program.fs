open Expecto

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common
open OpenCL.Net
open GraphBLAS.FSharp
open GraphBLAS.FSharp.IO

[<Tests>]
let allTests =
    testList "All tests" [
        //BackendTests.PrefixSum.tests
        //BackendTests.BitonicSort.tests
        BackendTests.RemoveDuplicates.tests
        BackendTests.Copy.tests
        BackendTests.Replicate.tests
        BackendTests.EwiseAdd.tests
        //Matrix.EWiseAdd.tests
        //Matrix.GetTuples.tests
        //Matrix.Mxv.tests
        //Matrix.Transpose.tests
        //Algo.Bfs.tests
    ]
    |> testSequenced

[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv
