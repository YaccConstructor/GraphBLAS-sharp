module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.UpperBound

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects.ClCellExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.UpperBound> ] }

let makeTest testFun (array: 'a [], value: 'a) =

    if array.Length > 0 then

        let array = Array.sort array

        let clArray = context.CreateClArray array
        let clValue = context.CreateClCell value

        let actual =
            (testFun processor clArray clValue: ClCell<_>)
                .ToHostAndFree processor

        let expected =
            let mutable expected = 0

            let array = Array.rev array

            for i in 0 .. array.Length - 1 do
                let currentValue = array.[i]

                if value < currentValue then
                    expected <- i

            array.Length - expected - 1

        "Results must be the same"
        |> Expect.equal actual expected

let createTest<'a when 'a: equality and 'a: comparison> =
    ClArray.upperBound<'a> context Constants.Common.defaultWorkGroupSize
    |> makeTest
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int>

      if Utils.isFloat64Available context.ClDevice then
          createTest<float>

      createTest<float32>
      createTest<bool> ]
    |> testList "UpperBound"
