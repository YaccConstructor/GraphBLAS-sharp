module Backend.RemoveDuplicates

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open GraphBLAS.FSharp.Backend.Common

let logger = Log.create "RemoveDuplicatesTests"

let testCases = [
    testCase "Simple correctness test" <| fun () ->
        let array = [| 1; 2; 2; 3; 3; 3 |]

        let actual =
            opencl {
                let! res = RemoveDuplicates.fromArray array
                let _ = ToHost res
                return res
            }
            |> OpenCLEvaluationContext().RunSync

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        let expected = [| 1; 2; 3 |]

        "Array should be without duplicates"
        |> Expect.sequenceEqual actual expected
]

let tests =
    testCases
    |> testList "Remove duplicates tests"
