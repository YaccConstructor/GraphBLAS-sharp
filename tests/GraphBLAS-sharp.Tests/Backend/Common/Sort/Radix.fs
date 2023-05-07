module GraphBLAS.FSharp.Tests.Backend.Common.Sort.Radix

open Expecto
open GraphBLAS.FSharp.Backend.Common.Sort
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects.ClContext

let config =
    { Utils.defaultConfig with
          startSize = 1000000 }

let workGroupSize = Utils.defaultWorkGroupSize

let processor = Context.defaultContext.Queue

let context = Context.defaultContext.ClContext

let checkResultByKeys (inputArray: (int * 'a) []) (actualValues: 'a []) =
    let expectedValues = Seq.sortBy fst inputArray |> Seq.map snd

    "Values must be the same"
    |> Expect.sequenceEqual expectedValues actualValues

let makeTestByKeys<'a when 'a: equality> sortFun (array: (int * 'a) []) =

    if array.Length > 0 then
        let keys = Array.map fst array
        let values = Array.map snd array

        let clKeys = keys.ToDevice context
        let clValues = values.ToDevice context

        let clActualValues: ClArray<'a> =
            sortFun processor HostInterop clKeys clValues

        let actualValues = clActualValues.ToHostAndFree processor

        checkResultByKeys array actualValues

let createTestByKeys<'a when 'a: equality and 'a: struct> =
    let sort =
        Radix.runByKeysStandard context workGroupSize

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
        Radix.standardRunKeysOnly context workGroupSize

    makeTestKeysOnly sort
    |> testPropertyWithConfig config $"keys only"

let allTests =
    testList "Radix" [ testKeysOnly; testByKeys ]
