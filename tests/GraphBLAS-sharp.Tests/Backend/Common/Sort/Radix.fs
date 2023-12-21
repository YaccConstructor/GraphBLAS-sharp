module GraphBLAS.FSharp.Tests.Backend.Common.Sort.Radix

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions

let config =
    { Utils.defaultConfig with
          startSize = 1000000 }

let workGroupSize =
    GraphBLAS.FSharp.Constants.Common.defaultWorkGroupSize

let processor = Context.defaultContext.Queue

let context = Context.defaultContext.ClContext

let checkResultByKeys (inputArray: (int * 'a) []) (actualKeys: int []) (actualValues: 'a []) =
    let expected = Seq.sortBy fst inputArray
    let expectedKeys = expected |> Seq.map fst
    let expectedValues = expected |> Seq.map snd

    "Keys must be the same"
    |> Expect.sequenceEqual expectedKeys actualKeys

    "Values must be the same"
    |> Expect.sequenceEqual expectedValues actualValues

let makeTestByKeys<'a when 'a: equality> sortFun (array: (int * 'a) []) =

    if array.Length > 0 then
        let keys = Array.map fst array
        let values = Array.map snd array

        let clKeys = keys.ToDevice context
        let clValues = values.ToDevice context

        let clActualKeys, clActualValues: ClArray<int> * ClArray<'a> =
            sortFun processor HostInterop clKeys clValues

        let actualKeys = clActualKeys.ToHostAndFree processor
        let actualValues = clActualValues.ToHostAndFree processor

        checkResultByKeys array actualKeys actualValues

let createTestByKeys<'a when 'a: equality and 'a: struct> =
    let sort =
        Sort.Radix.runByKeysStandard context workGroupSize

    makeTestByKeys<'a> sort
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let testByKeys =
    [ createTestByKeys<int>
      createTestByKeys<uint>

      if Utils.isFloat64Available context.ClDevice then
          createTestByKeys<float>

      createTestByKeys<float32>
      createTestByKeys<bool> ]
    |> testList "Radix sort by keys"

let makeTestKeysOnly sort (keys: uint []) =
    if keys.Length > 0 then
        let keys = Array.map int keys

        let clKeys = keys.ToDevice context

        let actual =
            (sort processor clKeys: ClArray<int>)
                .ToHostAndFree processor

        let expected = Array.sort keys

        "Keys must be the same"
        |> Expect.sequenceEqual expected actual

let testKeysOnly =
    let sort =
        Sort.Radix.standardRunKeysOnly context workGroupSize

    makeTestKeysOnly sort
    |> testPropertyWithConfig config $"keys only"

let allTests =
    testList "Radix" [ testKeysOnly; testByKeys ]
