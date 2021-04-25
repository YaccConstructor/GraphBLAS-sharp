open Expecto
open FsCheck

[<Tests>]
let allTests =
    testList "All tests" [
        //EWiseAdd.tests
        Mxm.tests
        // Vxm.tests
    ]
    |> testSequenced

[<EntryPoint>]
let main argv =
    allTests
    |> runTestsWithCLIArgs [] argv
