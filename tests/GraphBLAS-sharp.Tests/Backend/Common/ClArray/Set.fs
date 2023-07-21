module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Set

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Objects.ArraysExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.ClArray.Set> ] }

let makeTest<'a when 'a: equality> testFun (array: 'a [], position, value: 'a) =

    if array.Length > 0 then

        let clArray = context.CreateClArray array

        testFun processor clArray position value

        let actual = clArray.ToHostAndFree processor
        Array.set array position value

        "Results must be the same"
        |> Utils.compareArrays (=) actual array

let createTest<'a when 'a: equality> =
    ClArray.set context Utils.defaultWorkGroupSize
    |> makeTest<'a>
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int>

      if Utils.isFloat64Available context.ClDevice then
          createTest<float>

      createTest<float32>
      createTest<bool> ]
    |> testList "Set"
