module GraphBLAS.FSharp.Tests.Backend.Common.Scan.ByKey

open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ClContext
open Expecto
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let checkResult isEqual keysAndValues actual hostScan =

    let expected =
        HostPrimitives.scanByKey hostScan keysAndValues

    "Results must be the same"
    |> Utils.compareArrays isEqual actual expected

let makeTestSequentialSegments isEqual scanHost scanDevice (keysAndValues: (int * 'a) []) =
    if keysAndValues.Length > 0 then
        let keys, values =
            Array.sortBy fst keysAndValues |> Array.unzip

        let offsets =
            HostPrimitives.getUniqueBitmapFirstOccurrence keys
            |> HostPrimitives.getBitPositions

        let uniqueKeysCount = Array.distinct keys |> Array.length

        let clKeys =
            context.CreateClArrayWithSpecificAllocationMode(HostInterop, keys)

        let clValues =
            context.CreateClArrayWithSpecificAllocationMode(HostInterop, values)

        let clOffsets =
            context.CreateClArrayWithSpecificAllocationMode(HostInterop, offsets)

        scanDevice processor uniqueKeysCount clValues clKeys clOffsets

        let actual = clValues.ToHostAndFree processor
        clKeys.Free processor
        clOffsets.Free processor

        let keysAndValues = Array.zip keys values

        checkResult isEqual keysAndValues actual scanHost

let createTest (zero: 'a) opAddQ opAdd isEqual deviceScan hostScan =

    let hostScan = hostScan zero opAdd

    let deviceScan =
        deviceScan opAddQ zero context Utils.defaultWorkGroupSize

    makeTestSequentialSegments isEqual hostScan deviceScan
    |> testPropertyWithConfig Utils.defaultConfig $"test on {typeof<'a>}"

let sequentialSegmentsTests =
    let excludeTests =
        [ createTest 0 <@ (+) @> (+) (=) PrefixSum.ByKey.sequentialExclude HostPrimitives.prefixSumExclude

          if Utils.isFloat64Available context.ClDevice then
              createTest
                  0.0
                  <@ (+) @>
                  (+)
                  Utils.floatIsEqual
                  PrefixSum.ByKey.sequentialExclude
                  HostPrimitives.prefixSumExclude

          createTest
              0.0f
              <@ (+) @>
              (+)
              Utils.float32IsEqual
              PrefixSum.ByKey.sequentialExclude
              HostPrimitives.prefixSumExclude

          createTest false <@ (||) @> (||) (=) PrefixSum.ByKey.sequentialExclude HostPrimitives.prefixSumExclude
          createTest 0u <@ (+) @> (+) (=) PrefixSum.ByKey.sequentialExclude HostPrimitives.prefixSumExclude ]
        |> testList "exclude"

    let includeTests =
        [ createTest 0 <@ (+) @> (+) (=) PrefixSum.ByKey.sequentialInclude HostPrimitives.prefixSumInclude

          if Utils.isFloat64Available context.ClDevice then
              createTest
                  0.0
                  <@ (+) @>
                  (+)
                  Utils.floatIsEqual
                  PrefixSum.ByKey.sequentialInclude
                  HostPrimitives.prefixSumInclude

          createTest
              0.0f
              <@ (+) @>
              (+)
              Utils.float32IsEqual
              PrefixSum.ByKey.sequentialInclude
              HostPrimitives.prefixSumInclude

          createTest false <@ (||) @> (||) (=) PrefixSum.ByKey.sequentialInclude HostPrimitives.prefixSumInclude
          createTest 0u <@ (+) @> (+) (=) PrefixSum.ByKey.sequentialInclude HostPrimitives.prefixSumInclude ]

        |> testList "include"

    testList "Sequential segments" [ excludeTests; includeTests ]

let tests =
    testList "ByKey" [ sequentialSegmentsTests ]
