module GraphBLAS.FSharp.Tests.Backend.Common.Scatter

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let logger = Log.create "Scatter.Tests"

let context = defaultContext.ClContext

let config =
    { Tests.Utils.defaultConfig with
          endSize = 1000000 }

let wgSize = Tests.Utils.defaultWorkGroupSize

let q = defaultContext.Queue

let makeTest scatter (array: (int * 'a) []) (result: 'a []) =
    if array.Length > 0 then
        let positions, values = Array.unzip array

        let expected =
            Array.copy result
            |> HostPrimitives.scatter positions values

        let actual =
            use clPositions = context.CreateClArray positions
            use clValues = context.CreateClArray values
            use clResult = context.CreateClArray result

            scatter q clPositions clValues clResult

            clResult.ToHostAndFree q

        $"Arrays should be equal. Actual is \n%A{actual}, expected \n%A{expected}"
        |> Tests.Utils.compareArrays (=) actual expected

let testFixtures<'a when 'a: equality> =
    Scatter.runInplace<'a> context wgSize
    |> makeTest
    |> testPropertyWithConfig { config with endSize = 10 } $"Correctness on %A{typeof<'a>}"

let tests =
    q.Error.Add(fun e -> failwithf $"%A{e}")

    [ testFixtures<int>
      testFixtures<byte>
      testFixtures<bool> ]
    |> testList "Backend.Common.Scatter tests"
