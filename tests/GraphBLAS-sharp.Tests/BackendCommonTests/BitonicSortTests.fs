module Backend.BitonicSort

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend.Common
open Brahma.FSharp
open GraphBLAS.FSharp.Tests.Utils
open OpenCL.Net

let logger = Log.create "BitonicSort.Tests"

let testContext =
    ""
    |> avaliableContexts
    |> Seq.filter
        (fun context ->
            let mutable e = ErrorCode.Unknown
            let device = context.ClContext.ClDevice.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu)
    |> Seq.tryHead

let makeTest (context: ClContext) (q: MailboxProcessor<_>) sort (filter: 'a -> bool) (array: ('n * 'n * 'a) []) =
    let projection (row: 'n) (col: 'n) (v: 'a) = row, col

    let rows, cols, vals =
        array
        |> Array.distinctBy ((<|||) projection)
        |> Array.filter (fun (_, _, v) -> filter v)
        |> Array.unzip3

    if rows.Length > 0 then
        logger.debug (
            eventX "Initial size is {size}"
            >> setField "size" (sprintf "%A" rows.Length)
        )

        // logger.debug (
        //     eventX "Initial are {rows}, {cols}, {vals}"
        //     >> setField "rows" (sprintf "%A" rows)
        //     >> setField "cols" (sprintf "%A" cols)
        //     >> setField "vals" (sprintf "%A" vals)
        // )

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

        // logger.debug (
        //     eventX "Actual are {actualRows}, {actualCols}, {actualVals}"
        //     >> setField "actualRows" (sprintf "%A" actualRows)
        //     >> setField "actualCols" (sprintf "%A" actualCols)
        //     >> setField "actualVals" (sprintf "%A" actualVals)
        // )

        let expectedRows, expectedCols, expectedVals =
            (rows, cols, vals)
            |||> Array.zip3
            |> Array.sortBy ((<|||) projection)
            |> Array.unzip3

        (sprintf "Row arrays should be equal. Actual is \n%A, expected \n%A, input is \n%A" actualRows expectedRows rows)
        |> Expect.sequenceEqual actualRows expectedRows

        (sprintf
            "Column arrays should be equal. Actual is \n%A, expected \n%A, input is \n%A"
            actualCols
            expectedCols
            cols)
        |> Expect.sequenceEqual actualCols expectedCols

        (sprintf
            "Value arrays should be equal. Actual is \n%A, expected \n%A, input is \n%A"
            actualVals
            expectedVals
            vals)
        |> Expect.sequenceEqual actualVals expectedVals

let testFixtures<'a when 'a: equality> config wgSize context q filter =
    let sort: MailboxProcessor<_> -> ClArray<int> -> ClArray<int> -> ClArray<'a> -> unit =
        BitonicSort.sortKeyValuesInplace context wgSize

    makeTest context q sort filter
    |> testPropertyWithConfig config (sprintf "Correctness on %A" typeof<'a>)

let tests =
    match testContext with
    | Some c ->
        let context = c.ClContext
        let config = defaultConfig

        let wgSize = 128
        let q = c.Queue
        q.Error.Add(fun e -> failwithf "%A" e)

        [ testFixtures<int> config wgSize context q (fun _ -> true)
          testFixtures<float> config wgSize context q (System.Double.IsNaN >> not)
          testFixtures<byte> config wgSize context q (fun _ -> true)
          testFixtures<bool> config wgSize context q (fun _ -> true) ]
    | _ -> []
    |> testList "Backend.Common.BitonicSort tests"
