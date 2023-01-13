module GraphBLAS.FSharp.Tests.Backend.Common.Sum

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open FSharp.Quotations
open Context

let logger = Log.create "Sum.Test"

let context = defaultContext.ClContext

let makeTest (q: MailboxProcessor<_>) sum plus zero isEqual (filter: 'a [] -> 'a []) (array: 'a []) =
    if array.Length > 0 then
        let array = filter array

        logger.debug (
            eventX "Filtered array is {array}\n"
            >> setField "array" (sprintf "%A" array)
        )

        let actualSum =
            use clArray = context.CreateClArray array
            use total = sum q clArray

            let actualSum = [| zero |]
            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(total, actualSum, ch)).[0]

        logger.debug (
            eventX "Actual is {actual}\n"
            >> setField "actual" (sprintf "%A" actualSum)
        )

        let expectedSum = array |> Array.fold plus zero

        logger.debug (
            eventX "Expected is {expected}\n"
            >> setField "expected" (sprintf "%A" expectedSum)
        )

        "Total sums should be equal"
        |> Expect.equal actualSum expectedSum

let testFixtures config wgSize q plus (plusQ: Expr<'a -> 'a -> 'a>) zero isEqual filter name =
    let sum = Fold.sum context wgSize plusQ zero

    makeTest q sum plus zero isEqual filter
    |> testPropertyWithConfig config (sprintf "Correctness on %s" name)

let tests =
    let config = defaultConfig

    let wgSize = 128
    let q = defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    let filterFloats =
        Array.filter (System.Double.IsNaN >> not)

    [ testFixtures config wgSize q (+) <@ (+) @> 0 (=) id "int add"
      testFixtures config wgSize q (+) <@ (+) @> 0uy (=) id "byte add"
      testFixtures config wgSize q max <@ max @> 0 (=) id "int max"
      testFixtures config wgSize q max <@ max @> 0.0 (=) filterFloats "float max"
      testFixtures config wgSize q max <@ max @> 0uy (=) id "byte max"
      testFixtures config wgSize q min <@ min @> System.Int32.MaxValue (=) id "int min"
      testFixtures config wgSize q min <@ min @> System.Double.MaxValue (=) filterFloats "float min"
      testFixtures config wgSize q min <@ min @> System.Byte.MaxValue (=) id "byte min"
      testFixtures config wgSize q (||) <@ (||) @> false (=) id "bool logic-or"
      testFixtures config wgSize q (&&) <@ (&&) @> true (=) id "bool logic-and" ]
    |> testList "Backend.Common.Sum tests"
