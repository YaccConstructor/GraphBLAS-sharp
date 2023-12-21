module GraphBLAS.FSharp.Tests.Backend.Vector.Merge

open Brahma.FSharp
open Expecto
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ArraysExtensions

let processor = Context.defaultContext.Queue

let context = Context.defaultContext.ClContext

let config = Utils.defaultConfig

let makeTest isEqual zero testFun (firstArray: 'a []) (secondArray: 'a []) =
    let firstVector =
        Vector.Sparse.FromArray(firstArray, isEqual zero)

    let secondVector =
        Vector.Sparse.FromArray(secondArray, isEqual zero)

    if firstVector.NNZ > 0 && secondVector.NNZ > 0 then

        // actual run
        let clFirstVector = firstVector.ToDevice context

        let clSecondVector = secondVector.ToDevice context

        let ((allIndices: ClArray<int>),
             (firstValues: ClArray<'a>),
             (secondValues: ClArray<'a>),
             (isLeftBitmap: ClArray<int>)) =
            testFun processor clFirstVector clSecondVector

        clFirstVector.Dispose processor
        clSecondVector.Dispose processor

        let actualIndices = allIndices.ToHostAndFree processor
        let actualFirstValues = firstValues.ToHostAndFree processor
        let actualSecondValues = secondValues.ToHostAndFree processor
        let actualIsLeftBitmap = isLeftBitmap.ToHostAndFree processor

        let actualValues =
            (actualFirstValues, actualSecondValues, actualIsLeftBitmap)
            |||> Array.map3
                     (fun leftValue rightValue isLeft ->
                         if isLeft = 1 then
                             leftValue
                         else
                             rightValue)

        // expected run
        let firstValuesAndIndices =
            Array.map2 (fun value index -> (value, index)) firstVector.Values firstVector.Indices

        let secondValuesAndIndices =
            Array.map2 (fun value index -> (value, index)) secondVector.Values secondVector.Indices

        // preserve order of values then use stable sort
        let allValuesAndIndices =
            Array.concat [ firstValuesAndIndices
                           secondValuesAndIndices ]

        // stable sort
        let expectedValues, expectedIndices =
            Seq.sortBy snd allValuesAndIndices
            |> Seq.toArray
            |> Array.unzip

        "Values should be the same"
        |> Utils.compareArrays isEqual actualValues expectedValues

        "Indices should be the same"
        |> Utils.compareArrays (=) actualIndices expectedIndices

let createTest<'a when 'a: struct> isEqual (zero: 'a) =
    Vector.Sparse.Merge.run context Constants.Common.defaultWorkGroupSize
    |> makeTest isEqual zero
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int> (=) 0

      if Utils.isFloat64Available context.ClDevice then
          createTest<float> (=) 0.0

      createTest<float32> Utils.float32IsEqual 0.0f
      createTest<bool> (=) false ]
    |> testList "Merge"
