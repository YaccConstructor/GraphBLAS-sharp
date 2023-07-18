module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Item

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClCellExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.ClArray.Item> ] }

let makeTest<'a when 'a: equality> testFun (array: 'a [], position) =

    if array.Length > 0 then

        let clArray = context.CreateClArray array

        let result: ClCell<'a> = testFun processor position clArray

        clArray.Free processor
        let actual = result.ToHost processor

        let expected = Array.item position array

        "Results must be the same"
        |> Expect.equal actual expected

let createTest<'a when 'a: equality> =
    ClArray.item context Utils.defaultWorkGroupSize
    |> makeTest<'a>
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int>

      if Utils.isFloat64Available context.ClDevice then
          createTest<float>

      createTest<float32>
      createTest<bool> ]
    |> testList "Item"
