module GraphBLAS.FSharp.Tests.Common.Backend.ClArray.Pairwise

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.ArrayAndChunkPositions> ] }

let makeTest<'a> isEqual testFun (array: 'a [] ) =
    if array.Length > 0 then

        let clArray = context.CreateClArray array

        let (clFirstActual: ClArray<_>), (clSecondActual: ClArray<_>)
            = testFun processor HostInterop clArray

        let firstActual = clFirstActual.ToHostAndFree processor
        let secondActual = clSecondActual.ToHostAndFree processor

        let firstExpected, secondExpected =
            Array.pairwise array
            |> Array.unzip

        "First results must be the same"
        |> Utils.compareArrays isEqual firstActual firstExpected

        "Second results must be the same"
        |> Utils.compareArrays isEqual secondActual secondExpected

let createTest<'a> isEqual =
    ClArray.pairwise context Utils.defaultWorkGroupSize
    |> makeTest isEqual
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int> (=)

      if Utils.isFloat64Available context.ClDevice then
        createTest<float> (=)

      createTest<float32> (=)
      createTest<bool> (=) ]
