module Backend.PrefixSum

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open GraphBLAS.FSharp.Backend.Common

let logger = Log.create "PrefixSumTests"

let testCases = [
    testCase "Simple correctness test" <| fun () ->
        let array = [| 1; 2; 3 |]

        let actual =
            opencl {
                let! (res, total) = PrefixSum.run array
                let _ = ToHost res
                return res
            }
            |> OpenCLEvaluationContext().RunSync

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        let expected = [| 1; 3; 6 |]

        "Array should be without duplicates"
        |> Expect.sequenceEqual actual expected
]

let tests =
    testCases
    |> testList "Prefix sum tests"
