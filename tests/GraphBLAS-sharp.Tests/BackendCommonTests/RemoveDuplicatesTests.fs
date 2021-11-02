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
    let removeDuplicates_wg_1 = ClArray.removeDuplications context 1
    let removeDuplicates_wg_2 = ClArray.removeDuplications context 2
    let removeDuplicates_wg_32 = ClArray.removeDuplications context 32
    let q = context.Provider.CommandQueue
    q.Error.Add(fun e -> failwithf "%A" e)

    [ testCase "Simple correctness test"
      <| fun () ->
          let array = [| 1; 2; 2; 3; 3; 3 |]

          let clArray = context.CreateClArray array

          let actual =
              let clActual = removeDuplicates_wg_2 q clArray

              let actual = Array.zeroCreate clActual.Length
              q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clActual, actual, ch))

          logger.debug (
              eventX "Actual is {actual}"
              >> setField "actual" (sprintf "%A" actual)
          )

          let expected = [| 1; 2; 3 |]

          "Array should be without duplicates"
          |> Expect.sequenceEqual actual expected

      testProperty "Correctness test on random int arrays"
      <| fun (array: array<int>) ->
          let array = Array.sort array
          printfn "array: %A" array

          if array.Length > 0 then
              let clArray = context.CreateClArray array

              let removeDuplicates =
                  if array.Length % 32 = 0 then
                      removeDuplicates_wg_32
                  else
                      removeDuplicates_wg_2

              let actual =
                  let clActual = removeDuplicates q clArray

                  let actual = Array.zeroCreate clActual.Length
                  q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clActual, actual, ch))

              logger.debug (
                  eventX "Actual is {actual}"
                  >> setField "actual" (sprintf "%A" actual)
              )

              let expected = Seq.distinct array |> Array.ofSeq

              "Array should be without duplicates"
              |> Expect.sequenceEqual actual expected

      ]

let tests =
    testCases
    |> testList "Array.removeDuplicates tests"
