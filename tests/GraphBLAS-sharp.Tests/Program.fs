open Expecto
open FsCheck

[<Tests>]
let allTests =
    testList "All tests" [
        EWiseAdd.tests
    ]

// sequenced test?
[<EntryPoint>]
let main argv =
    allTests
    |> testSequenced
    |> runTestsWithCLIArgs [] argv
