module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Fill

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContextExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.Fill> ] }

let makeTest<'a> isEqual testFun (value: 'a, targetPosition, count, target: 'a []) =
    if target.Length > 0 then

        let clTarget = context.CreateClArray target
        let clValue = context.CreateClCell value

        testFun processor clValue targetPosition count clTarget

        // release
        let actual = clTarget.ToHostAndFree processor

        // write to target
        Array.fill target targetPosition count value

        "Results must be the same"
        |> Utils.compareArrays isEqual actual target

let createTest<'a> isEqual =
    ClArray.fill context Utils.defaultWorkGroupSize
    |> makeTest<'a> isEqual
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int> (=)

      if Utils.isFloat64Available context.ClDevice then
          createTest<float> (=)

      createTest<float32> (=)
      createTest<bool> (=) ]
    |> testList "Fill"
