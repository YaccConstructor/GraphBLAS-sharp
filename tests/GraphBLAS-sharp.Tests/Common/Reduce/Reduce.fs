module GraphBLAS.FSharp.Tests.Backend.Common.Reduce

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests

let logger = Log.create "Reduce.Tests"

let context = Context.defaultContext.ClContext

let config = Utils.defaultConfig

let wgSize = Utils.defaultWorkGroupSize

let q = Context.defaultContext.Queue

let makeTest (reduce: MailboxProcessor<_> -> ClArray<'a> -> ClCell<'a>) plus zero (array: 'a []) =

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

let testFixtures plus plusQ zero name =
    let reduce = Reduce.reduce context wgSize plusQ

    makeTest reduce plus zero
    |> testPropertyWithConfig config $"Correctness on %s{name}"

let tests =
    q.Error.Add(fun e -> failwithf "%A" e)

    [ testFixtures (+) <@ (+) @> 0 "int add"
      testFixtures (+) <@ (+) @> 0uy "byte add"
      testFixtures max <@ max @> System.Int32.MinValue "int max"
      testFixtures max <@ max @> System.Byte.MinValue "byte max"
      testFixtures min <@ min @> System.Int32.MaxValue "int min"

      if Utils.isFloat64Available context.ClDevice then
          testFixtures max <@ max @> System.Double.MinValue "float max"
          testFixtures min <@ min @> System.Double.MaxValue "float min"

      testFixtures max <@ max @> System.Single.MinValue "float32 max"
      testFixtures min <@ min @> System.Single.MaxValue "float32 min"

      testFixtures min <@ min @> System.Byte.MaxValue "byte min"
      testFixtures (||) <@ (||) @> false "bool logic-or"
      testFixtures (&&) <@ (&&) @> true "bool logic-and" ]
    |> testList "Backend.Common.Reduce tests"
