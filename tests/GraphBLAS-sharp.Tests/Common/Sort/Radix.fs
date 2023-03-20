module GraphBLAS.FSharp.Tests.Backend.Common.Sort.Radix

open Expecto
open GraphBLAS.FSharp.Backend.Common.Sort
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Brahma.FSharp

let config = { Utils.defaultConfig with startSize = 1000000 }

let workGroupSize = Utils.defaultWorkGroupSize

let processor =  Context.defaultContext.Queue

let context = Context.defaultContext.ClContext

let checkResult (inputArray: (int * 'a) []) (actualKeys: int []) (actualValues: 'a []) =
    let expectedKeys, expectedValues =
        Array.sortBy fst inputArray
        |> Array.unzip

    "Keys must be the same"
    |>Expect.sequenceEqual expectedKeys actualKeys

    "Values must be the same"
    |>Expect.sequenceEqual expectedValues actualValues

let makeTest<'a when 'a : equality> sortFun (array: (int * 'a) []) =
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

        checkResult array actualKeys actualValues

let createTest<'a when 'a : equality and 'a : struct> =
    let sort = Radix.run1DInplaceStandard context workGroupSize

    makeTest<'a> sort
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let testFixtures =
    [ createTest<int>
      createTest<uint>

      if Utils.isFloat64Available context.ClDevice then
        createTest<float>

      createTest<float32>
      createTest<bool> ]


