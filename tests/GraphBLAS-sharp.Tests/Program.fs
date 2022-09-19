open Expecto

open Brahma.FSharp

[<Tests>]
let allTests =
    testList "All tests" [
        BackendTests.PrefixSum.tests
        BackendTests.BitonicSort.tests
        BackendTests.RemoveDuplicates.tests
        BackendTests.Copy.tests
        BackendTests.Replicate.tests
        BackendTests.Convert.tests
        BackendTests.EwiseAdd.tests
        BackendTests.EwiseAdd.tests2
        BackendTests.EwiseAddBatched.tests
    ]
    |> testSequenced

[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv
