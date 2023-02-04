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

let config = defaultConfig

let wgSize = 128
let q = defaultContext.Queue

let makeTest sum plus zero (array: 'a []) =
    if array.Length > 0 then

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

let testFixtures plus (plusQ: Expr<'a -> 'a -> 'a>) zero name =
    let sum = Reduce.sum context wgSize plusQ zero

    makeTest sum plus zero
    |> testPropertyWithConfig config (sprintf "Correctness on %s" name)

let tests =

    q.Error.Add(fun e -> failwithf "%A" e)

    [ testFixtures (+) <@ (+) @> 0 "int add"
      testFixtures (+) <@ (+) @> 0uy "byte add"
      testFixtures max <@ max @> 0 "int max"
      testFixtures max <@ max @> 0.0 "float max"
      testFixtures max <@ max @> 0uy "byte max"
      testFixtures min <@ min @> System.Int32.MaxValue "int min"
      testFixtures min <@ min @> System.Double.MaxValue "float min"
      testFixtures min <@ min @> System.Byte.MaxValue "byte min"
      testFixtures (||) <@ (||) @> false "bool logic-or"
      testFixtures (&&) <@ (&&) @> true "bool logic-and" ]
    |> testList "Backend.Common.Sum tests"
