open Expecto
open FsCheck

[<Tests>]
let allTests =
    testList "All tests" [
        EWiseAdd.tests
    ]

[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv
