module GraphBLAS.FSharp.Tests.Backend.Common.Reduce

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "Reduce.Tests"

let context = Context.defaultContext.ClContext

let makeTest
    (q: MailboxProcessor<_>)
    (reduce: MailboxProcessor<_> -> ClArray<'a> -> ClCell<'a>)
    plus
    zero
    (filter: 'a [] -> 'a [])
    (array: 'a [])
    =

    let array = filter array

    if array.Length > 0 then
        let reduce = reduce q

        logger.debug (
            eventX "Filtered array is {array}\n"
            >> setField "array" (sprintf "%A" array)
        )

        let actualSum =
            use clArray = context.CreateClArray array
            let total = reduce clArray

            let actualSum = [| zero |]

            let sum =
                q.PostAndReply(fun ch -> Msg.CreateToHostMsg(total, actualSum, ch))

            sum.[0]

        logger.debug (
            eventX "Actual is {actual}\n"
            >> setField "actual" (sprintf "%A" actualSum)
        )

        let expectedSum = Array.fold plus zero array

        logger.debug (
            eventX "Expected is {expected}\n"
            >> setField "expected" (sprintf "%A" expectedSum)
        )

        "Total sums should be equal"
        |> Expect.equal actualSum expectedSum


let testFixtures config wgSize q plus plusQ zero filter name =
    let reduce = Reduce.run context wgSize plusQ

    makeTest q reduce plus zero filter
    |> testPropertyWithConfig config (sprintf "Correctness on %s" name)

let tests =
    let config = defaultConfig

    let wgSize = 32
    let q = Context.defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    let filterFloats = Array.filter System.Double.IsNormal

    [ testFixtures config wgSize q (+) <@ (+) @> 0 id "int add"
      testFixtures config wgSize q (+) <@ (+) @> 0uy id "byte add"
      testFixtures config wgSize q max <@ max @> System.Int32.MinValue id "int max"
      testFixtures config wgSize q max <@ max @> System.Double.MinValue filterFloats "float max"
      testFixtures config wgSize q max <@ max @> System.Byte.MinValue id "byte max"
      testFixtures config wgSize q min <@ min @> System.Int32.MaxValue id "int min"
      testFixtures config wgSize q min <@ min @> System.Double.MaxValue filterFloats "float min"
      testFixtures config wgSize q min <@ min @> System.Byte.MaxValue id "byte min"
      testFixtures config wgSize q (||) <@ (||) @> false id "bool logic-or"
      testFixtures config wgSize q (&&) <@ (&&) @> true id "bool logic-and" ]
    |> testList "Backend.Common.Reduce tests"
