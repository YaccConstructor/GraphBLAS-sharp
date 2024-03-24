module GraphBLAS.FSharp.Tests.Backend.Common.Scan.PrefixSum

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Objects.ClCellExtensions
open GraphBLAS.FSharp.Objects.ArraysExtensions

let logger = Log.create "ClArray.PrefixSum.Tests"

let context = defaultContext.ClContext

let config =
    { Tests.Utils.defaultConfig with
          maxTest = 20
          startSize = 1
          endSize = 1000000 }

let wgSize = 128

let q = defaultContext.Queue

let makeTest plus zero isEqual scanInclude scanExclude (array: 'a []) =
    if array.Length > 0 then
        // Exclude
        let actual, actualSum =
            let clArray = context.CreateClArray array
            let (total: ClCell<_>) = scanExclude q clArray

            let actual = clArray.ToHostAndFree q
            let actualSum = total.ToHostAndFree q

            actual, actualSum

        let expected, expectedSum =
            array
            |> Array.mapFold
                (fun s t ->
                    let a = plus s t
                    s, a)
                zero

        "Arrays for exclude should be the same"
        |> Tests.Utils.compareArrays isEqual actual expected

        "Total sums for exclude should be equal"
        |> Expect.equal actualSum expectedSum

        // Include
        let actual, actualSum =
            let clArray = context.CreateClArray array
            let (total: ClCell<_>) = scanInclude q clArray zero

            let actual = clArray.ToHostAndFree q
            let actualSum = total.ToHostAndFree q
            actual, actualSum

        let expected, expectedSum =
            array
            |> Array.mapFold
                (fun s t ->
                    let a = plus s t
                    a, a)
                zero

        "Total sums for include should be equal"
        |> Expect.equal actualSum expectedSum

        "Arrays for include should be the same"
        |> Tests.Utils.compareArrays isEqual actual expected

let testFixtures plus plusQ zero isEqual name =
    (PrefixSum.runIncludeInPlace plusQ context wgSize, PrefixSum.runExcludeInPlace plusQ zero context wgSize)
    ||> makeTest plus zero isEqual
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
