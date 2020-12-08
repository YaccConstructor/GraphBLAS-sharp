namespace GraphBLAS.FSharp.Tests

open Expecto

module Main =
    [<Tests>]
    let allTests =
        testList "All Tests" [

        ]

    [<EntryPoint>]
    let main argv =
        allTests
        |> runTestsWithCLIArgs [] argv

