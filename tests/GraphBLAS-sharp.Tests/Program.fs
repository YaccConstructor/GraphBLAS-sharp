open Expecto
open FsCheck

[<Tests>]
let allTests =
    testList "All Tests" [

    ]

[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv
