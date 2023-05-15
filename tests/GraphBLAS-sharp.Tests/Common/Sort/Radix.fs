namespace GraphBLAS.FSharp.Tests.Backend.Common.Sort

open Expecto
open GraphBLAS.FSharp.Backend.Common.Sort
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Brahma.FSharp

module Radix =
    let config =
        { Utils.defaultConfig with
              startSize = 1000000 }

    let workGroupSize = Utils.defaultWorkGroupSize

    let processor = Context.defaultContext.Queue

    let context = Context.defaultContext.ClContext

    let checkResultByKeys (inputArray: (int * 'a) []) (actualKeys: int []) (actualValues: 'a []) =
        let expected =
            Array.sortBy fst inputArray
        let expectedKeys =
            Array.map fst expected
        let expectedValues =
            Array.map snd expected

        "Keys must be the same"
        |> Expect.sequenceEqual expectedKeys actualKeys
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

            let clActualKeys, clActualValues: ClArray<int> * ClArray<'a> = sortFun processor clKeys clValues

            let actualKeys = clActualKeys.ToHostAndFree processor
            let actualValues = clActualValues.ToHostAndFree processor

            checkResultByKeys array actualKeys actualValues

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

    let testsByKeys =
        testList "Radix sort by keys" testFixturesByKeys

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
            Radix.standardRunKeysOnly context workGroupSize

        makeTestKeysOnly sort
        |> testPropertyWithConfig config $"keys only"
