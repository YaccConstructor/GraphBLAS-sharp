open Expecto
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Tests


[<EntryPoint>]
let main argv = Common.ClArray.UpperBound.tests |> testSequenced |> runTestsWithCLIArgs [] argv
