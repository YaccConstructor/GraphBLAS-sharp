module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.RemoveDuplicates

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let logger = Log.create "RemoveDuplicates.Tests"

let context = Context.defaultContext.ClContext

let testCases =
    let removeDuplicates_wg_2 = ClArray.removeDuplications context 2
    let removeDuplicates_wg_32 = ClArray.removeDuplications context 32
    let q = Context.defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    [ testCase "Simple correctness test"
      <| fun () ->
          let array = [| 1; 2; 2; 3; 3; 3 |]

          let clArray = context.CreateClArray array

          let actual =
              (removeDuplicates_wg_2 q clArray).ToHostAndFree q

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

          if array.Length > 0 then
              let clArray = context.CreateClArray array

              let removeDuplicates =
                  if array.Length % 32 = 0 then
                      removeDuplicates_wg_32
                  else
                      removeDuplicates_wg_2

              let actual =
                  (removeDuplicates q clArray).ToHostAndFree q

              logger.debug (
                  eventX "Actual is {actual}"
                  >> setField "actual" $"%A{actual}"
              )

              let expected = Seq.distinct array |> Array.ofSeq

              "Array should be without duplicates"
              |> Expect.sequenceEqual actual expected ]

let tests =
    testCases
    |> testList "Array.removeDuplicates tests"
