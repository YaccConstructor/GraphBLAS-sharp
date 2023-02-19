module GraphBLAS.FSharp.Tests.Backend.Common.PrefixSum

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "ClArray.PrefixSum.Tests"

let context = defaultContext.ClContext

let config = defaultConfig

let wgSize = 128

let q = defaultContext.Queue

let makeTest scan plus zero isEqual (array: 'a []) =
    if array.Length > 0 then

        logger.debug (
            eventX $"Array is %A{array}\n"
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

        "Total sums should be equal"
        |> Expect.equal actualSum expectedSum

        "Arrays should be the same"
        |> compareArrays isEqual actual expected

let testFixtures plus plusQ zero isEqual name =
    let scan =
        ClArray.prefixSumIncludeInplace plusQ context wgSize

    makeTest scan plus zero isEqual
    |> testPropertyWithConfig config (sprintf "Correctness on %s" name)

let tests =
    q.Error.Add(fun e -> failwithf "%A" e)

    [ testFixtures (+) <@ (+) @> 0 (=) "int add"
      testFixtures (+) <@ (+) @> 0uy (=) "byte add"
      testFixtures max <@ max @> 0 (=) "int max"
      testFixtures max <@ max @> 0.0 (=) "float max"
      testFixtures max <@ max @> 0uy (=) "byte max"
      testFixtures min <@ min @> System.Int32.MaxValue (=) "int min"
      testFixtures min <@ min @> System.Double.MaxValue (=) "float min"
      testFixtures min <@ min @> System.Byte.MaxValue (=) "byte min"
      testFixtures (||) <@ (||) @> false (=) "bool logic-or"
      testFixtures (&&) <@ (&&) @> true (=) "bool logic-and" ]
    |> testList "Backend.Common.PrefixSum tests"
