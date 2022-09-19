module BackendTests.BitonicSort

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend.Common
open Brahma.FSharp
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "BitonicSort.Tests"

let makeTest (context: ClContext) (q: MailboxProcessor<_>) sort (array: ('n * 'n * 'a) []) =
    if array.Length > 0 then
        let projection (row: 'n) (col: 'n) (v: 'a) = row, col

        logger.debug (
            eventX "Initial size is {size}"
            >> setField "size" $"%A{array.Length}"
        )

        let rows, cols, vals = Array.unzip3 array

        use clRows = context.CreateClArray rows
        use clCols = context.CreateClArray cols
        use clVals = context.CreateClArray vals

        let actualRows, actualCols, actualVals =
            sort q clRows clCols clVals

            let rows = Array.zeroCreate<'n> clRows.Length
            let cols = Array.zeroCreate<'n> clCols.Length
            let vals = Array.zeroCreate<'a> clVals.Length

            q.Post(Msg.CreateToHostMsg(clRows, rows))
            q.Post(Msg.CreateToHostMsg(clCols, cols))

            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clVals, vals, ch))
            |> ignore

            rows, cols, vals

        let expectedRows, expectedCols, expectedVals =
            (rows, cols, vals)
            |||> Array.zip3
            |> Array.sortBy ((<|||) projection)
            |> Array.unzip3

        $"Row arrays should be equal. Actual is \n%A{actualRows}, expected \n%A{expectedRows}, input is \n%A{rows}"
        |> compareArrays (=) actualRows expectedRows

        $"Column arrays should be equal. Actual is \n%A{actualCols}, expected \n%A{expectedCols}, input is \n%A{cols}"
        |> compareArrays (=) actualCols expectedCols

        $"Value arrays should be equal. Actual is \n%A{actualVals}, expected \n%A{expectedVals}, input is \n%A{vals}"
        |> compareArrays (=) actualVals expectedVals

let testFixtures<'a when 'a: equality> config wgSize context q =
    let sort: MailboxProcessor<_> -> ClArray<int> -> ClArray<int> -> ClArray<'a> -> unit =
        BitonicSort.sortKeyValuesInplace context wgSize

    makeTest context q sort
    |> testPropertyWithConfig config $"Correctness on %A{typeof<'a>}"

let tests =
    let context = defaultContext.ClContext
    let config = { defaultConfig with endSize = 1000000 }

    let wgSize = 32
    let q = defaultContext.Queue

    [
        testFixtures<int> config wgSize context q
        testFixtures<float> config wgSize context q
        testFixtures<byte> config wgSize context q
        testFixtures<bool> config wgSize context q
    ]
    |> testList "Backend.Common.BitonicSort tests"
