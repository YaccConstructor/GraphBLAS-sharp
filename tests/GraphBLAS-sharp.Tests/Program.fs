open Expecto

[<Tests>]
let allTests =
    testList "All tests" [
        Backend.PrefixSum.tests
        Backend.BitonicSort.tests
        Backend.RemoveDuplicates.tests
        Matrix.Mxv.tests
        // Matrix.Transpose.tests
    ]
    |> testSequenced

[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv
