module Backend.PrefixSum

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common

let logger = Log.create "PrefixSum.Tests"

let testCases =
    [ testCase "Simple correctness test"
      <| fun () ->
          let array = [| 1; 2; 3 |]

          let actual =
              opencl {
                  let! (result, _) = PrefixSum.runInclude array

                  if array.Length <> 0 then
                      let! _ = ToHost result
                      ()

                  return result
              }
              |> OpenCLEvaluationContext().RunSync

          logger.debug (
              eventX "Actual is {actual}"
              >> setField "actual" (sprintf "%A" actual)
          )

          let expected = [| 1; 3; 6 |]

          "Array should be without duplicates"
          |> Expect.sequenceEqual actual expected

      testCase "Test on empty array"
      <| fun () ->
          let array = Array.zeroCreate<int> 0

          let actual =
              opencl {
                  let! (result, _) = PrefixSum.runInclude array

                  if array.Length <> 0 then
                      let! _ = ToHost result
                      ()

                  return result
              }
              |> OpenCLEvaluationContext().RunSync

          logger.debug (
              eventX "Actual is {actual}"
              >> setField "actual" (sprintf "%A" actual)
          )

          let expected = array

          "Array should be without duplicates"
          |> Expect.sequenceEqual actual expected ]

let tests = testCases |> testList "PrefixSum tests"
