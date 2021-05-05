open Expecto

[<Tests>]
let allTests =
    testList "All tests" [
        Backend.PrefixSum.tests
        Backend.BitonicSort.tests
        Backend.RemoveDuplicates.tests
        Matrix.GetTuples.tests
        Matrix.Mxv.tests
        Matrix.EWiseAdd.tests
        // Matrix.Transpose.tests
    ]
    |> testSequenced

[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv
