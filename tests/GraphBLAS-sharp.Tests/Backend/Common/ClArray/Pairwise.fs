module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Pairwise

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.BufferCompatibleArray> ] }

let makeTest<'a> isEqual testFun (array: 'a []) =
    if array.Length > 0 then

        let clArray = context.CreateClArray array

        match testFun processor HostInterop clArray with
        | Some (actual: ClArray<_>) ->
            let actual = actual.ToHostAndFree processor

            let expected = Array.pairwise array

            "First results must be the same"
            |> Utils.compareArrays isEqual actual expected
        | None ->
            "Result must be empty"
            |> Expect.isTrue (array.Size <= 1)

let createTest<'a> isEqual =
    ClArray.pairwise context Utils.defaultWorkGroupSize
    |> makeTest<'a> isEqual
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int> (=)

      if Utils.isFloat64Available context.ClDevice then
          createTest<float> (=)

      createTest<float32> (=)
      createTest<bool> (=) ]
    |> testList "Pairwise"
