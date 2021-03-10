open Expecto
open FsCheck

[<Tests>]
let allTests =
    testList "All tests" [
        EWiseAdd.tests
    ]
    |> testSequenced

// sequenced test?
[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv
