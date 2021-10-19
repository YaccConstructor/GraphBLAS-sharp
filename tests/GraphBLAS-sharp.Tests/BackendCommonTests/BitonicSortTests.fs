module Backend.BitonicSort

open Expecto
open FsCheck
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Predefined
open TypeShape.Core
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open OpenCL.Net
open GraphBLAS.FSharp.Backend.Common

let logger = Log.create "BitonicSort.Tests"

let testCases =
    [ let config = Utils.defaultConfig

      ptestPropertyWithConfig config "Simple correctness test on uint64 * int"
      <| fun (array: (uint64 * int) []) ->
          let expected = Array.sortBy (fun (key, _) -> key) array

          let actual =
              opencl {
                  let (keys, values) = Array.unzip array
                  do! BitonicSort.sortKeyValuesInplace keys values

                  if array.Length <> 0 then

                      let! _ = ToHost keys
                      let! _ = ToHost values
                      ()

                  return keys, values
              }
              |> OpenCLEvaluationContext().RunSync
              ||> Array.zip

          "Actual array should be equal to sorted"
          |> Expect.sequenceEqual actual expected ]

let tests =
    testCases |> testList "BitonicSort tests"
