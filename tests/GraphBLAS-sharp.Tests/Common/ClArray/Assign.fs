module GraphBLAS.FSharp.Tests.Backned.Common.ClArray.Assign

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
          arbitrary = [ typeof<Generators.ArrayAndChunkPositions> ] }

let makeTest<'a> isEqual testFun (source: 'a []) (target: 'a []) =

    if source.Length > 0
        && target.Length > 0 then

        let clSource = context.CreateClArray source
        let clTarget = context.CreateClArray target
        let targetPosition = 0

        testFun processor clSource targetPosition clTarget

        let actual = clSource.ToHostAndFree processor
        clTarget.Free processor

        // write to target --- target expected
        Array.blit source 0 target targetPosition source.Length

        "Results should be the same"
        |> Utils.compareArrays isEqual actual target

let createTest<'a when 'a : equality> isEqual =
    ClArray.assign context Utils.defaultWorkGroupSize
    |> makeTest isEqual
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int> (=)

      if Utils.isFloat64Available context.ClDevice then
        createTest<float> (=)

      createTest<float32> (=)
      createTest<bool> (=) ]
    |> testList "Assign"
