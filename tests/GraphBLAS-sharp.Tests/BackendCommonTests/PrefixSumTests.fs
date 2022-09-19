module BackendTests.PrefixSum

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "PrefixSum.Tests"

let context = defaultContext.ClContext

let makeTest (q: MailboxProcessor<_>) scan plus zero isEqual (filter: 'a [] -> 'a []) (array: 'a []) =
    if array.Length > 0 then
        let array = filter array

        logger.debug (
            eventX "Filtered array is {array}\n"
            >> setField "array" (sprintf "%A" array)
        )

        let actual, actualSum =
            use clArray = context.CreateClArray array
            use total = context.CreateClCell()
            scan q clArray total zero |> ignore

            let actual = Array.zeroCreate<'a> clArray.Length
            let actualSum = [| zero |]
            q.Post(Msg.CreateToHostMsg(total, actualSum))
            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clArray, actual, ch)), actualSum.[0]

        logger.debug (
            eventX "Actual is {actual}\n"
            >> setField "actual" (sprintf "%A" actual)
        )

        let expected, expectedSum =
            array
            |> Array.mapFold
                (fun s t ->
                    let a = plus s t
                    a, a)
                zero

        logger.debug (
            eventX "Expected is {expected}\n"
            >> setField "expected" (sprintf "%A" expected)
        )

        "Lengths of arrays should be equal"
        |> Expect.equal actual.Length expected.Length

        "Total sums should be equal"
        |> Expect.equal actualSum expectedSum

        for i in 0 .. actual.Length - 1 do
            Expect.isTrue
                (isEqual actual.[i] expected.[i])
                (sprintf "Arrays should be the same. Actual is \n%A, expected \n%A, input is \n%A" actual expected array)

let testFixtures config wgSize q plus plusQ zero isEqual filter name =
    let scan =
        PrefixSum.runIncludeInplace plusQ context wgSize

    makeTest q scan plus zero isEqual filter
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
    |> testList "Backend.Common.PrefixSum tests"
