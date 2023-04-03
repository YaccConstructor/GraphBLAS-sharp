module GraphBLAS.FSharp.Tests.Backend.Common.Reduce.ByKey

open Expecto
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ClContext
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config = Utils.defaultConfig

let checkResult isEqual actualKeys actualValues keys values reduceOp =

    let expectedKeys, expectedValues =
        HostPrimitives.reduceByKey keys values reduceOp

    "Keys must be the same"
    |> Utils.compareArrays (=) actualKeys expectedKeys

    "Values must the same"
    |> Utils.compareArrays isEqual actualValues expectedValues

let makeTest isEqual reduce reduceOp (arrayAndKeys: (int * 'a) []) =
    let keys, values =
        Array.sortBy fst arrayAndKeys |> Array.unzip

    if keys.Length > 0 then
        let clKeys =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, keys)

        let clValues =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, values)

        let resultLength = Array.length <| Array.distinct keys

        let clActualKeys, clActualValues: ClArray<int> * ClArray<'a> =
            reduce processor HostInterop resultLength clKeys clValues

        clValues.Free processor
        clKeys.Free processor

        let actualValues = clActualValues.ToHostAndFree processor
        let actualKeys = clActualKeys.ToHostAndFree processor

        checkResult isEqual actualKeys actualValues keys values reduceOp

let createTestSequential<'a> (isEqual: 'a -> 'a -> bool) reduceOp reduceOpQ =

    let reduce =
        Reduce.ByKey.sequential context Utils.defaultWorkGroupSize reduceOpQ

    makeTest isEqual reduce reduceOp
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let sequentialTest =
    let addTests =
        testList
            "add tests"
            [ createTestSequential<int> (=) (+) <@ (+) @>
              createTestSequential<byte> (=) (+) <@ (+) @>

              if Utils.isFloat64Available context.ClDevice then
                  createTestSequential<float> Utils.floatIsEqual (+) <@ (+) @>

              createTestSequential<float32> Utils.float32IsEqual (+) <@ (+) @>
              createTestSequential<bool> (=) (||) <@ (||) @> ]

    let mulTests =
        testList
            "mul tests"
            [ createTestSequential<int> (=) (*) <@ (*) @>
              createTestSequential<byte> (=) (*) <@ (*) @>

              if Utils.isFloat64Available context.ClDevice then
                  createTestSequential<float> Utils.floatIsEqual (*) <@ (*) @>

              createTestSequential<float32> Utils.float32IsEqual (*) <@ (*) @>
              createTestSequential<bool> (=) (&&) <@ (&&) @> ]

    testList "Sequential" [ addTests; mulTests ]

let createTestOneWorkGroup<'a> (isEqual: 'a -> 'a -> bool) reduceOp reduceOpQ =
    let reduce =
        Reduce.ByKey.oneWorkGroupSegments context Utils.defaultWorkGroupSize reduceOpQ

    makeTest isEqual reduce reduceOp
    |> testPropertyWithConfig
        { config with
              endSize = Utils.defaultWorkGroupSize }
        $"test on {typeof<'a>}"

let oneWorkGroupTest =
    let addTests =
        testList
            "add tests"
            [ createTestOneWorkGroup<int> (=) (+) <@ (+) @>
              createTestOneWorkGroup<byte> (=) (+) <@ (+) @>

              if Utils.isFloat64Available context.ClDevice then
                  createTestOneWorkGroup<float> Utils.floatIsEqual (+) <@ (+) @>

              createTestOneWorkGroup<float32> Utils.float32IsEqual (+) <@ (+) @>
              createTestOneWorkGroup<bool> (=) (||) <@ (||) @> ]

    let mulTests =
        testList
            "mul tests"
            [ createTestOneWorkGroup<int> (=) (*) <@ (*) @>
              createTestOneWorkGroup<byte> (=) (*) <@ (*) @>

              if Utils.isFloat64Available context.ClDevice then
                  createTestOneWorkGroup<float> Utils.floatIsEqual (*) <@ (*) @>

              createTestOneWorkGroup<float32> Utils.float32IsEqual (*) <@ (*) @>
              createTestOneWorkGroup<bool> (=) (&&) <@ (&&) @> ]

    testList "One work group" [ addTests; mulTests ]

let makeTestSequentialSegments isEqual reduce reduceOp (valuesAndKeys: (int * 'a) []) =

    let valuesAndKeys = Array.sortBy fst valuesAndKeys

    if valuesAndKeys.Length > 0 then
        let offsets =
            Array.map fst valuesAndKeys
            |> HostPrimitives.getUniqueBitmapFirstOccurrence
            |> HostPrimitives.getBitPositions

        let resultLength = offsets.Length

        let keys, values = Array.unzip valuesAndKeys

        let clOffsets =
            context.CreateClArrayWithSpecificAllocationMode(HostInterop, offsets)

        let clKeys =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, keys)

        let clValues =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, values)

        let clReducedKeys, clReducedValues: ClArray<int> * ClArray<'a> =
            reduce processor DeviceOnly resultLength clOffsets clKeys clValues

        let reducedKeys = clReducedKeys.ToHostAndFree processor
        let reducedValues = clReducedValues.ToHostAndFree processor

        checkResult isEqual reducedKeys reducedValues keys values reduceOp


let createTestSequentialSegments<'a> (isEqual: 'a -> 'a -> bool) reduceOp reduceOpQ =
    let reduce =
        Reduce.ByKey.segmentSequential context Utils.defaultWorkGroupSize reduceOpQ

    makeTestSequentialSegments isEqual reduce reduceOp
    |> testPropertyWithConfig { config with startSize = 1000 } $"test on {typeof<'a>}"

let sequentialSegmentTests =
    let addTests =
        testList
            "add tests"
            [ createTestSequentialSegments<int> (=) (+) <@ (+) @>
              createTestSequentialSegments<byte> (=) (+) <@ (+) @>

              if Utils.isFloat64Available context.ClDevice then
                  createTestSequentialSegments<float> Utils.floatIsEqual (+) <@ (+) @>

              createTestSequentialSegments<float32> Utils.float32IsEqual (+) <@ (+) @>
              createTestSequentialSegments<bool> (=) (||) <@ (||) @> ]

    let mulTests =
        testList
            "mul tests"
            [ createTestSequentialSegments<int> (=) (*) <@ (*) @>
              createTestSequentialSegments<byte> (=) (*) <@ (*) @>

              if Utils.isFloat64Available context.ClDevice then
                  createTestSequentialSegments<float> Utils.floatIsEqual (*) <@ (*) @>

              createTestSequentialSegments<float32> Utils.float32IsEqual (*) <@ (*) @>
              createTestSequentialSegments<bool> (=) (&&) <@ (&&) @> ]

    testList "Sequential segments" [ addTests; mulTests ]

let checkResult2D isEqual firstActualKeys secondActualKeys actualValues firstKeys secondKeys values reduceOp =

    let expectedFirstKeys, expectedSecondKeys, expectedValues =
        HostPrimitives.reduceByKey2D firstKeys secondKeys values reduceOp

    "First keys must be the same"
    |> Utils.compareArrays (=) firstActualKeys expectedFirstKeys

    "Second keys must be the same"
    |> Utils.compareArrays (=) secondActualKeys expectedSecondKeys

    "Values must the same"
    |> Utils.compareArrays isEqual actualValues expectedValues

let makeTest2D isEqual reduce reduceOp (array: (int * int * 'a) []) =
    let firstKeys, secondKeys, values =
        array
        |> Array.sortBy (fun (fst, snd, _) -> fst, snd)
        |> Array.unzip3

    if firstKeys.Length > 0 then
        let clFirstKeys =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, firstKeys)

        let clSecondKeys =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, secondKeys)

        let clValues =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, values)

        let resultLength = Array.length <| Array.distinctBy (fun (fst, snd, _) -> (fst, snd)) array

        let clFirstActualKeys, clSecondActualKeys, clActualValues: ClArray<int> * ClArray<int> * ClArray<'a> =
            reduce processor HostInterop resultLength clFirstKeys clSecondKeys clValues

        clValues.Free processor
        clFirstKeys.Free processor
        clSecondKeys.Free processor

        let actualValues = clActualValues.ToHostAndFree processor
        let firstActualKeys = clFirstActualKeys.ToHostAndFree processor
        let secondActualKeys = clSecondActualKeys.ToHostAndFree processor

        checkResult2D isEqual firstActualKeys secondActualKeys actualValues firstKeys secondKeys values reduceOp

let createTestSequential2D<'a> (isEqual: 'a -> 'a -> bool) reduceOp reduceOpQ =

    let reduce =
        Reduce.ByKey2D.sequential context Utils.defaultWorkGroupSize reduceOpQ

    makeTest2D isEqual reduce reduceOp
    |> testPropertyWithConfig { config with arbitrary = [ typeof<Generators.ArrayOfDistinctKeys> ]; endSize = 10 } $"test on {typeof<'a>}"

let sequential2DTest =
    let addTests =
        testList
            "add tests"
            [ createTestSequential2D<int> (=) (+) <@ (+) @>
              createTestSequential2D<byte> (=) (+) <@ (+) @>

              if Utils.isFloat64Available context.ClDevice then
                  createTestSequential2D<float> Utils.floatIsEqual (+) <@ (+) @>

              createTestSequential2D<float32> Utils.float32IsEqual (+) <@ (+) @>
              createTestSequential2D<bool> (=) (||) <@ (||) @> ]

    let mulTests =
        testList
            "mul tests"
            [ createTestSequential2D<int> (=) (*) <@ (*) @>
              createTestSequential2D<byte> (=) (*) <@ (*) @>

              if Utils.isFloat64Available context.ClDevice then
                  createTestSequential2D<float> Utils.floatIsEqual (*) <@ (*) @>

              createTestSequential2D<float32> Utils.float32IsEqual (*) <@ (*) @>
              createTestSequential2D<bool> (=) (&&) <@ (&&) @> ]

    testList "Sequential 2D" [ addTests; mulTests ]

let makeTestSequentialSegments2D isEqual reduce reduceOp (array: (int * int * 'a) []) =

    if array.Length > 0 then
        let array = Array.sortBy (fun (fst, snd, _) -> fst, snd) array

        let offsets =
            array
            |> Array.map (fun (fst, snd, _) -> fst, snd)
            |> HostPrimitives.getUniqueBitmapFirstOccurrence
            |> HostPrimitives.getBitPositions

        let resultLength = offsets.Length

        let firstKeys, secondKeys, values = Array.unzip3 array

        let clOffsets =
            context.CreateClArrayWithSpecificAllocationMode(HostInterop, offsets)

        let clFirstKeys =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, firstKeys)

        let clSecondKeys =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, secondKeys)

        let clValues =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, values)

        let clFirstActualKeys, clSecondActualKeys, clReducedValues: ClArray<int> * ClArray<int> * ClArray<'a> =
            reduce processor DeviceOnly resultLength clOffsets clFirstKeys clSecondKeys clValues

        let reducedFirsKeys = clFirstActualKeys.ToHostAndFree processor
        let reducesSecondKeys = clSecondActualKeys.ToHostAndFree processor
        let reducedValues = clReducedValues.ToHostAndFree processor

        checkResult2D isEqual reducedFirsKeys reducesSecondKeys reducedValues firstKeys secondKeys values reduceOp

let createTestSequentialSegments2D<'a> (isEqual: 'a -> 'a -> bool) reduceOp reduceOpQ =
    let reduce =
        Reduce.ByKey2D.segmentSequential context Utils.defaultWorkGroupSize reduceOpQ

    makeTestSequentialSegments2D isEqual reduce reduceOp
    |> testPropertyWithConfig { config with arbitrary = [ typeof<Generators.ArrayOfDistinctKeys> ] } $"test on {typeof<'a>}"

let sequentialSegmentTests2D =
    let addTests =
        testList
            "add tests"
            [ createTestSequentialSegments2D<int> (=) (+) <@ (+) @>
              createTestSequentialSegments2D<byte> (=) (+) <@ (+) @>

              if Utils.isFloat64Available context.ClDevice then
                  createTestSequentialSegments2D<float> Utils.floatIsEqual (+) <@ (+) @>

              createTestSequentialSegments2D<float32> Utils.float32IsEqual (+) <@ (+) @>
              createTestSequentialSegments2D<bool> (=) (||) <@ (||) @> ]

    let mulTests =
        testList
            "mul tests"
            [ createTestSequentialSegments2D<int> (=) (*) <@ (*) @>
              createTestSequentialSegments2D<byte> (=) (*) <@ (*) @>

              if Utils.isFloat64Available context.ClDevice then
                  createTestSequentialSegments2D<float> Utils.floatIsEqual (*) <@ (*) @>

              createTestSequentialSegments2D<float32> Utils.float32IsEqual (*) <@ (*) @>
              createTestSequentialSegments2D<bool> (=) (&&) <@ (&&) @> ]

    testList "Sequential segments 2D" [ addTests; mulTests ]
