module GraphBLAS.FSharp.Tests.Backend.Common.BitonicSort

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend.Common
open Brahma.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context

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

        use clRows = context.CreateClArray rows
        use clColumns = context.CreateClArray cols
        use clValues = context.CreateClArray vals

        let actualRows, actualCols, actualValues =
            sort q clRows clColumns clValues

            let rows = Array.zeroCreate<'n> clRows.Length
            let columns = Array.zeroCreate<'n> clColumns.Length
            let values = Array.zeroCreate<'a> clValues.Length

            q.Post(Msg.CreateToHostMsg(clRows, rows))
            q.Post(Msg.CreateToHostMsg(clColumns, columns))

            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clValues, values, ch))
            |> ignore

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
    BitonicSort.sortKeyValuesInplace<int, 'a> context wgSize
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
