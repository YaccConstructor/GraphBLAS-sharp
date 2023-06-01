module GraphBLAS.FSharp.Tests.Backend.Common.Scan.PrefixSum

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let logger = Log.create "ClArray.PrefixSum.Tests"

let context = defaultContext.ClContext

let config = Tests.Utils.defaultConfig

let wgSize = 128

let q = defaultContext.Queue

let makeTest plus zero isEqual scan (array: 'a []) =
    if array.Length > 0 then

        logger.debug (
            eventX $"Array is %A{array}\n"
            >> setField "array" (sprintf "%A" array)
        )

        let actual, actualSum =
            let clArray = context.CreateClArray array
            let (total: ClCell<_>) = scan q clArray

            let actual = clArray.ToHostAndFree q
            let actualSum = total.ToHostAndFree q
            actual, actualSum

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
        |> Tests.Utils.compareArrays isEqual actual expected

let testFixtures plus plusQ zero isEqual name =
    PrefixSum.runIncludeInPlace plusQ zero context wgSize
    |> makeTest plus zero isEqual
    |> testPropertyWithConfig config $"Correctness on %s{name}"

let tests =
    q.Error.Add(fun e -> failwithf "%A" e)

    [ testFixtures (+) <@ (+) @> 0 (=) "int add"
      testFixtures (+) <@ (+) @> 0uy (=) "byte add"
      testFixtures max <@ max @> 0 (=) "int max"
      testFixtures max <@ max @> 0uy (=) "byte max"
      testFixtures min <@ min @> System.Int32.MaxValue (=) "int min"

      if Tests.Utils.isFloat64Available context.ClDevice then
          testFixtures min <@ min @> System.Double.MaxValue (=) "float min"
          testFixtures max <@ max @> 0.0 (=) "float max"

      testFixtures min <@ min @> System.Single.MaxValue (=) "float32 min"
      testFixtures max <@ max @> 0.0f (=) "float32 max"

      testFixtures min <@ min @> System.Byte.MaxValue (=) "byte min"
      testFixtures (||) <@ (||) @> false (=) "bool logic-or"
      testFixtures (&&) <@ (&&) @> true (=) "bool logic-and" ]
    |> testList "Backend.Common.PrefixSum tests"
