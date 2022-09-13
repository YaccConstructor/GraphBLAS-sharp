open Expecto

open Brahma.FSharp

[<Tests>]
let allTests =
    testList "All tests" [
//        BackendTests.PrefixSum.tests
//        BackendTests.BitonicSort.tests
        BackendTests.RemoveDuplicates.tests
        BackendTests.Copy.tests
        BackendTests.Replicate.tests
        BackendTests.EwiseAdd.tests
        BackendTests.EwiseAddBatched.tests
//        Matrix.EWiseAdd.tests
//        Matrix.GetTuples.tests
//        Matrix.Mxv.tests
//        Matrix.Transpose.tests
//        Algo.Bfs.tests
    ]
    |> testSequenced

[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv
