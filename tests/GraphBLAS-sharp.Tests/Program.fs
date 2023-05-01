open Expecto
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Tests


[<EntryPoint>]
let main argv = Backend.Matrix.SubRows.tests |> testSequenced |> runTestsWithCLIArgs [] argv
