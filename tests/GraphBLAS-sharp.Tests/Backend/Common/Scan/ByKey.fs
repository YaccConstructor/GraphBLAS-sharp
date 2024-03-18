module GraphBLAS.FSharp.Tests.Backend.Common.Scan.ByKey

open Expecto
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions

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
        deviceScan opAddQ zero context Constants.Common.defaultWorkGroupSize

    makeTestSequentialSegments isEqual hostScan deviceScan
    |> testPropertyWithConfig Utils.defaultConfig $"test on {typeof<'a>}"

let sequentialSegmentsTests =
    let excludeTests =
        [ createTest 0 <@ (+) @> (+) (=) Common.PrefixSum.ByKey.sequentialExclude HostPrimitives.prefixSumExclude

          if Utils.isFloat64Available context.ClDevice then
              createTest
                  0.0
                  <@ (+) @>
                  (+)
                  Utils.floatIsEqual
                  Common.PrefixSum.ByKey.sequentialExclude
                  HostPrimitives.prefixSumExclude

          createTest
              0.0f
              <@ (+) @>
              (+)
              Utils.float32IsEqual
              Common.PrefixSum.ByKey.sequentialExclude
              HostPrimitives.prefixSumExclude

          createTest false <@ (||) @> (||) (=) Common.PrefixSum.ByKey.sequentialExclude HostPrimitives.prefixSumExclude
          createTest 0u <@ (+) @> (+) (=) Common.PrefixSum.ByKey.sequentialExclude HostPrimitives.prefixSumExclude ]
        |> testList "exclude"

    let includeTests =
        [ createTest 0 <@ (+) @> (+) (=) Common.PrefixSum.ByKey.sequentialInclude HostPrimitives.prefixSumInclude

          if Utils.isFloat64Available context.ClDevice then
              createTest
                  0.0
                  <@ (+) @>
                  (+)
                  Utils.floatIsEqual
                  Common.PrefixSum.ByKey.sequentialInclude
                  HostPrimitives.prefixSumInclude

          createTest
              0.0f
              <@ (+) @>
              (+)
              Utils.float32IsEqual
              Common.PrefixSum.ByKey.sequentialInclude
              HostPrimitives.prefixSumInclude

          createTest false <@ (||) @> (||) (=) Common.PrefixSum.ByKey.sequentialInclude HostPrimitives.prefixSumInclude
          createTest 0u <@ (+) @> (+) (=) Common.PrefixSum.ByKey.sequentialInclude HostPrimitives.prefixSumInclude ]

        |> testList "include"

    testList "Sequential segments" [ excludeTests; includeTests ]

let tests =
    testList "ByKey" [ sequentialSegmentsTests ]
