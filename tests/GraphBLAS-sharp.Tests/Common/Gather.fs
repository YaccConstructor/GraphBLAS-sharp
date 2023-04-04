module GraphBLAS.FSharp.Tests.Backend.Common.Gather

open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests
open Expecto
open Microsoft.FSharp.Collections
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

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

    let testFun = testFun context Utils.defaultWorkGroupSize

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
