module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Blit

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.Blit> ] }

let makeTest<'a> isEqual testFun (source: 'a [], sourceIndex, target: 'a [], targetIndex, count) =

    if source.Length > 0 && target.Length > 0 then

        let clSource = context.CreateClArray source
        let clTarget = context.CreateClArray target

        testFun processor clSource sourceIndex clTarget targetIndex count

        clSource.Free processor
        let actual = clTarget.ToHostAndFree processor

        // write to target --- target expected
        Array.blit source sourceIndex target targetIndex count

        "Results should be the same"
        |> Utils.compareArrays isEqual actual target

let createTest<'a when 'a: equality> isEqual =
    ClArray.blit context Utils.defaultWorkGroupSize
    |> makeTest<'a> isEqual
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int> (=)

      if Utils.isFloat64Available context.ClDevice then
          createTest<float> Utils.floatIsEqual

      createTest<float32> Utils.float32IsEqual
      createTest<bool> (=) ]
    |> testList "Blit"
