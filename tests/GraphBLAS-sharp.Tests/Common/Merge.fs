module GraphBLAS.FSharp.Tests.Common.Merge

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          endSize = 10000000 }

let makeTest isEqual testFun (leftArray: 'a []) (rightArray: 'a []) =
    if leftArray.Length > 0 && rightArray.Length > 0 then

        let leftArray = Array.sort leftArray |> Array.distinct

        let rightArray = Array.sort rightArray |> Array.distinct

        let clLeftArray = context.CreateClArray leftArray
        let clRightArray = context.CreateClArray rightArray

        let clResult: ClArray<'a> =
            testFun processor clLeftArray clRightArray

        let result = clResult.ToHostAndFree processor
        clLeftArray.Free processor
        clRightArray.Free processor

        let expected =
            Array.concat [ leftArray; rightArray ]
            |> Array.sort

        "Results must be the same"
        |> Utils.compareArrays isEqual result expected

let createTest<'a> isEqual =
    Merge.run context Utils.defaultWorkGroupSize
    |> makeTest isEqual
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let tests =
    [ createTest<int> (=)

      if Utils.isFloat64Available context.ClDevice then
          createTest<float> (=)

      createTest<float32> (=)
      createTest<bool> (=) ]
    |> testList "Merge"
