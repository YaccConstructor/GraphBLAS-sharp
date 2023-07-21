module GraphBLAS.FSharp.Tests.Backend.Common.Gather

open Expecto
open Microsoft.FSharp.Collections
open GraphBLAS.FSharp.Common
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Backend.Quotes

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let check isEqual actual positions values target =

    HostPrimitives.gather positions values target
    |> ignore

    "Results must be the same"
    |> Utils.compareArrays isEqual actual target

let makeTest isEqual testFun (array: (uint * 'a * 'a) []) =

    if array.Length > 0 then

        let positions, values, target =
            Array.unzip3 array
            |> fun (fst, snd, thd) -> Array.map int fst, snd, thd

        let clPositions =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, positions)

        let clValues =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, values)

        let clTarget =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, target)

        testFun processor clPositions clValues clTarget

        clPositions.Free processor
        clValues.Free processor

        let actual = clTarget.ToHostAndFree processor

        check isEqual actual positions values target

let createTest<'a> (isEqual: 'a -> 'a -> bool) testFun =

    let testFun =
        testFun context Utils.defaultWorkGroupSize

    makeTest isEqual testFun
    |> testPropertyWithConfig Utils.defaultConfig $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int> (=) Gather.run

      if Utils.isFloat64Available context.ClDevice then
          createTest<float> Utils.floatIsEqual Gather.run

      createTest<float32> Utils.float32IsEqual Gather.run
      createTest<bool> (=) Gather.run
      createTest<uint> (=) Gather.run ]
    |> testList "Gather"


let makeTestInit isEqual testFun indexMap (array: ('a * 'a) []) =
    if array.Length > 0 then

        let positions, values, target =
            Array.mapi (fun index (first, second) -> indexMap index, first, second) array
            |> Array.unzip3

        let clValues =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, values)

        let clTarget =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, target)

        testFun processor clValues clTarget

        clValues.Free processor

        let actual = clTarget.ToHostAndFree processor

        check isEqual actual positions values target

let createTestInit<'a> (isEqual: 'a -> 'a -> bool) testFun indexMapQ indexMap =

    let testFun =
        testFun indexMapQ context Utils.defaultWorkGroupSize

    makeTestInit isEqual testFun indexMap
    |> testPropertyWithConfig Utils.defaultConfig $"test on {typeof<'a>}"

let initTests =

    let idTests =
        [ createTestInit<int> (=) Gather.runInit Map.id id

          if Utils.isFloat64Available context.ClDevice then
              createTestInit<float> Utils.floatIsEqual Gather.runInit Map.id id

          createTestInit<float32> Utils.float32IsEqual Gather.runInit Map.id id
          createTestInit<bool> (=) Gather.runInit Map.id id
          createTestInit<uint> (=) Gather.runInit Map.id id ]
        |> testList "id"

    let inc = ((+) 1)

    let incTests =
        [ createTestInit<int> (=) Gather.runInit Map.inc inc

          if Utils.isFloat64Available context.ClDevice then
              createTestInit<float> Utils.floatIsEqual Gather.runInit Map.inc inc

          createTestInit<float32> Utils.float32IsEqual Gather.runInit Map.inc inc
          createTestInit<bool> (=) Gather.runInit Map.inc inc
          createTestInit<uint> (=) Gather.runInit Map.inc inc ]
        |> testList "inc"

    testList "init" [ idTests; incTests ]


let allTests = testList "Gather" [ tests; initTests ]
