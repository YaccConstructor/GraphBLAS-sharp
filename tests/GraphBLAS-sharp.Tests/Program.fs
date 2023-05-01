open Expecto
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Tests


[<EntryPoint>]
let main argv = Matrix.ExpandRows.tests |> testSequenced |> runTestsWithCLIArgs [] argv
