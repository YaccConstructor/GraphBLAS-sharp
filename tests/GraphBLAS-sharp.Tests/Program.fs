open Expecto
open FsCheck

[<Tests>]
let allTests =
    testList "All Tests" [
        EWiseAddTests.checkGeneric EWiseAddTests.reflexivity
    ]

[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv
