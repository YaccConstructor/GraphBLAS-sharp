module Backend.BitonicSort

open Expecto
open GraphBLAS.FSharp.Tests
open TypeShape.Core
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend.Common
open Brahma.FSharp
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "BitonicSort.Tests"

let testContext =
    let contexts = "" |> avaliableContexts |> Seq.toList
    contexts.[0]

let context = testContext.ClContext

let makeTest (q: MailboxProcessor<_>) sort (filter: 'a -> bool) (array: ('n * 'n * 'a) []) =
    if array.Length > 0 then
        let projection (row: 'n) (col: 'n) (v: 'a) = row, col

        let rows, cols, vals =
            array
            |> Array.distinctBy ((<|||) projection)
            |> Array.filter (fun (_, _, v) -> filter v)
            |> Array.unzip3

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

        logger.debug (
            eventX "Actual are {actualRows}, {actualCols}, {actualVals}"
            >> setField "actualRows" (sprintf "%A" actualRows)
            >> setField "actualCols" (sprintf "%A" actualCols)
            >> setField "actualVals" (sprintf "%A" actualVals)
        )

        let expectedRows, expectedCols, expectedVals =
            (rows, cols, vals)
            |||> Array.zip3
            |> Array.sortBy ((<|||) projection)
            |> Array.unzip3

        "Row arrays should be equal"
        |> Expect.sequenceEqual actualRows expectedRows

        "Column arrays should be equal"
        |> Expect.sequenceEqual actualCols expectedCols

        "Value arrays should be equal"
        |> Expect.sequenceEqual actualVals expectedVals

let testFixtures<'a when 'a: equality> config wgSize q filter =
    let sort: MailboxProcessor<_> -> ClArray<int> -> ClArray<int> -> ClArray<'a> -> unit =
        BitonicSort.sortKeyValuesInplace context wgSize

    makeTest q sort filter
    |> testPropertyWithConfig config (sprintf "Correctness on %A" typeof<'a>)

let tests =
    let config = defaultConfig

    let wgSize = 128
    let q = testContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    [ testFixtures<int> config wgSize q (fun _ -> true)
      testFixtures<float> config wgSize q (System.Double.IsNaN >> not)
      testFixtures<byte> config wgSize q (fun _ -> true)
      testFixtures<bool> config wgSize q (fun _ -> true) ]
    |> testList "Backend.Common.BitonicSort tests"
