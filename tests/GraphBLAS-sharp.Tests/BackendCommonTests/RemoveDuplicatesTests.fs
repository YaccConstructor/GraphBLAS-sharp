module Backend.RemoveDuplicates

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend

let logger = Log.create "RemoveDuplicates.Tests"

let context =
    let deviceType = ClDeviceType.Default
    let platformName = ClPlatform.Any
    ClContext(platformName, deviceType)

let testCases =
    [ testCase "Simple correctness test"
      <| fun () ->
          let q = context.Provider.CommandQueue
          let array = [| 1; 2; 2; 3; 3; 3 |]

          let clArray = context.CreateClArray array

          let actual =
              let clActual =
                  ClArray.removeDuplications context 2 q clArray

              let actual = Array.zeroCreate clActual.Length
              q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clActual, actual, ch))

          logger.debug (
              eventX "Actual is {actual}"
              >> setField "actual" (sprintf "%A" actual)
          )

          let expected = [| 1; 2; 3 |]

          "Array should be without duplicates"
          |> Expect.sequenceEqual actual expected ]

(*testCase "Test on empty array"
      <| fun () ->
          let array = Array.zeroCreate<int> 0

          let actual =
              opencl {
                  let! copiedArray = Copy.copyArray array
                  let! result = RemoveDuplicates.fromArray copiedArray

                  if array.Length <> 0 then
                      failwith "fix me"
                      //let! _ = ToHost result
                      ()

                  return result
              }
              //|> OpenCLEvaluationContext().RunSync
              failwith "fix me"

          logger.debug (
              eventX "Actual is {actual}"
              >> setField "actual" (sprintf "%A" actual)
          )

          let expected = array

          "Array should be without duplicates"
          |> Expect.sequenceEqual actual expected ]*)

let tests =
    testCases |> testList "RemoveDuplicates tests"
