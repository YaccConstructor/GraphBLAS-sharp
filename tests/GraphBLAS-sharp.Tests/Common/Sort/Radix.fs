module GraphBLAS.FSharp.Tests.Backend.Common.Sort.Radix

open Expecto
open GraphBLAS.FSharp.Backend.Common.Sort
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Brahma.FSharp

let config =
    { Utils.defaultConfig with
          startSize = 1000000 }

let workGroupSize = Utils.defaultWorkGroupSize

let processor = Context.defaultContext.Queue

let context = Context.defaultContext.ClContext

let checkResultByKeys (inputArray: (int * 'a) []) (actualValues: 'a []) =
    let expectedValues =
        Array.sortBy fst inputArray
        |> Array.map snd

    "Values must be the same"
    |> Expect.sequenceEqual expectedValues actualValues

let makeTestByKeys<'a when 'a: equality> sortFun (array: (int * 'a) []) =
    // since Array.sort not stable
    let array = Array.distinctBy fst array

    if array.Length > 0 then

        let keys = Array.map fst array
        let values = Array.map snd array

        let clKeys = keys.ToDevice context
        let clValues = values.ToDevice context

        let clActualValues: ClArray<'a> = sortFun processor clKeys clValues

        let actualValues = clActualValues.ToHostAndFree processor

        checkResultByKeys array actualValues

let createTestByKeys<'a when 'a: equality and 'a: struct> =
    let sort =
        Radix.runByKeysStandard context workGroupSize

    makeTestByKeys<'a> sort
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let testFixturesByKeys =
    [ createTestByKeys<int>
      createTestByKeys<uint>

      if Utils.isFloat64Available context.ClDevice then
          createTestByKeys<float>

      createTestByKeys<float32>
      createTestByKeys<bool> ]

let testsByKeys = testList "Radix sort by keys" testFixturesByKeys

let makeTestKeysOnly sort (keys: uint []) =
    if keys.Length > 0 then
        let keys = Array.map int keys

        let clKeys = keys.ToDevice context

        let actual = (sort processor clKeys: ClArray<int>).ToHostAndFree processor

        let expected = Array.sort keys

        "Keys must be the same"
        |> Expect.sequenceEqual expected actual

let createTestKeysOnly<'a when 'a : equality and 'a : struct> =
    let sort = Radix.runByKeysStandard context workGroupSize

    makeTestByKeys<'a> sort
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let testFixturesKeysOnly =
    [ createTestKeysOnly<int>
      createTestKeysOnly<uint>

      if Utils.isFloat64Available context.ClDevice then
          createTestKeysOnly<float>

      createTestKeysOnly<float32>
      createTestKeysOnly<bool> ]

let testsKeysOnly = testList "Radix sort keys only" testFixturesKeysOnly
