namespace GraphBLAS.FSharp.Tests.Backend.Common.Sort

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Common
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Objects.ArraysExtensions

module Bitonic =
    let logger = Log.create "BitonicSort.Tests"

    let context = defaultContext.ClContext

    let config =
        { Utils.defaultConfig with
              endSize = 1000000 }

    let wgSize = Utils.defaultWorkGroupSize

    let q = defaultContext.Queue

    let makeTest sort (array: ('n * 'n * 'a) []) =
        if array.Length > 0 then
            let projection (row: 'n) (col: 'n) (_: 'a) = row, col

            logger.debug (
                eventX "Initial size is {size}"
                >> setField "size" $"%A{array.Length}"
            )

            let rows, cols, vals = Array.unzip3 array

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

            $"Value arrays should be equal. Actual is \n%A{actualValues}, expected \n%A{expectedValues}, input is \n%A{vals}"
            |> Utils.compareArrays (=) actualValues expectedValues

    let testFixtures<'a when 'a: equality> =
        Bitonic.sortKeyValuesInplace context wgSize
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
