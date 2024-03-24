namespace GraphBLAS.FSharp.Tests.Backend.Common.Sort

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Objects.ArraysExtensions

module Bitonic =
    let logger = Log.create "BitonicSort.Tests"

    let context = defaultContext.ClContext

    let config =
        { Utils.defaultConfig with
              endSize = 1000000 }

    let wgSize =
        GraphBLAS.FSharp.Constants.Common.defaultWorkGroupSize

    let q = defaultContext.Queue

    let makeTest sort (array: (int * int * 'a) []) =
        if array.Length > 0 then
            let projection (row: 'n) (col: 'n) (_: 'a) = row, col

            logger.debug (
                eventX "Initial size is {size}"
                >> setField "size" $"%A{array.Length}"
            )

            let rows, cols, vals = Array.unzip3 array
            let rows = Array.map abs rows
            let cols = Array.map abs cols

            let clRows = context.CreateClArray rows
            let clColumns = context.CreateClArray cols
            let clValues = context.CreateClArray vals

            let actualRows, actualCols, actualValues =
                sort q clRows clColumns clValues

                let rows = clRows.ToHostAndFree q
                let columns = clColumns.ToHostAndFree q
                let values = clValues.ToHostAndFree q

                rows, columns, values

            let expectedRows, expectedCols, expectedValues =
                (rows, cols, vals)
                |||> Array.zip3
                |> Array.sortBy ((<|||) projection)
                |> Array.unzip3

            $"Row arrays should be equal. Actual is \n%A{actualRows}, expected \n%A{expectedRows}, input is \n%A{rows}"
            |> Utils.compareArrays (=) actualRows expectedRows

            $"Column arrays should be equal. Actual is \n%A{actualCols}, expected \n%A{expectedCols}, input is \n%A{cols}"
            |> Utils.compareArrays (=) actualCols expectedCols

            // Check that for each pair of equal keys values are the same
            let mutable i = 1

            let expected, actual =
                new ResizeArray<'a>(), new ResizeArray<'a>()

            expected.Add expectedValues.[0]
            actual.Add actualValues.[0]

            while i < expectedValues.Size do
                if
                    not
                        (
                            actualRows.[i - 1] = actualRows.[i]
                            && actualCols.[i - 1] = actualCols.[i]
                        )
                then
                    Expect.sequenceEqual
                        (actual |> Seq.countBy id)
                        (actual |> Seq.countBy id)
                        $"Values for keys %A{actualRows.[i - 1]}, %A{actualCols.[i - 1]} are not the same"

                    expected.Clear()
                    actual.Clear()

                expected.Add expectedValues.[i]
                actual.Add actualValues.[i]
                i <- i + 1

            Expect.sequenceEqual
                actual
                expected
                $"Values for keys %A{actualRows.[i - 1]}, %A{actualCols.[i - 1]} are not the same"

    let testFixtures<'a when 'a: equality> =
        Sort.Bitonic.sortKeyValuesInplace<'a> context wgSize
        |> makeTest
        |> testPropertyWithConfig config $"Correctness on %A{typeof<'a>}"

    let tests =
        q.Error.Add(fun e -> failwithf "%A" e)

        [ testFixtures<int>

          if Utils.isFloat64Available context.ClDevice then
              testFixtures<float>

          testFixtures<float32>
          testFixtures<byte>
          testFixtures<bool> ]
        |> testList "Backend.Common.BitonicSort tests"
