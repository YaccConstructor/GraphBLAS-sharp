module GraphBLAS.FSharp.Tests.Backend.Common.Scatter

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let logger = Log.create "Scatter.Tests"

let context = defaultContext.ClContext

let config = { Utils.defaultConfig with endSize = 10000 }

let wgSize = Utils.defaultWorkGroupSize

let q = defaultContext.Queue

let makeTest<'a when 'a: equality> hostScatter scatter (array: (int * 'a) []) (result: 'a []) =
    if array.Length > 0 then
        let positions, values = Array.unzip array

        let expected =
            Array.copy result
            |> hostScatter positions values

        let actual =
            let clPositions = context.CreateClArray positions
            use clValues = context.CreateClArray values
            use clResult = context.CreateClArray result

            scatter q clPositions clValues clResult

            clResult.ToHostAndFree q

        $"Arrays should be equal."
        |> Utils.compareArrays (=) actual expected

let testFixturesLast<'a when 'a: equality> =
    Scatter.scatterLastOccurrence context wgSize
    |> makeTest<'a> HostPrimitives.scatterLastOccurrence
    |> testPropertyWithConfig config $"Correctness on %A{typeof<'a>}"

let testFixturesFirst<'a when 'a: equality> =
    Scatter.scatterFirstOccurrence context wgSize
    |> makeTest<'a> HostPrimitives.scatterFirstOccurrence
    |> testPropertyWithConfig config $"Correctness on %A{typeof<'a>}"

let tests =
    q.Error.Add(fun e -> failwithf $"%A{e}")

    let last =
        [ testFixturesLast<int>
          testFixturesLast<byte>
          testFixturesLast<bool> ]
        |> testList "Last Occurrence"

    let first =
        [ testFixturesFirst<int>
          testFixturesFirst<byte>
          testFixturesFirst<bool> ]
        |> testList "First Occurrence"

    testList "Scatter tests" [first; last]

let makeTestInit<'a when 'a: equality> positionsMap scatter (values: 'a []) (result: 'a []) =
    if values.Length > 0 then

        let positionsAndValues =
            Array.mapi (fun index value -> positionsMap index, value) values

        let expected =
            Array.init result.Length (fun index ->
                match Array.tryFind (fst >> ((=) index)) positionsAndValues with
                | Some (_, value) -> value
                | None -> result.[index])

        let actual =
            let values = Array.map snd positionsAndValues

            use clValues = context.CreateClArray values
            use clResult = context.CreateClArray result

            scatter q clValues clResult

            clResult.ToHostAndFree q

        $"Arrays should be equal."
        |> Utils.compareArrays (=) actual expected

let createInitTest<'a when 'a: equality> indexMap indexMapQ =
    Scatter.init<'a> indexMapQ context Utils.defaultWorkGroupSize
    |> makeTestInit<'a> indexMap
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let initTests =
    q.Error.Add(fun e -> failwithf $"%A{e}")

    let idTest =
        [ createInitTest<int> id Map.id
          createInitTest<byte> id Map.id
          createInitTest<bool> id Map.id  ]
        |> testList "id"

    let inc = ((+) 1)

    let incTest =
        [ createInitTest<int> inc Map.inc
          createInitTest<byte> inc Map.inc
          createInitTest<bool> inc Map.inc ]
        |> testList "increment"

    testList "Scatter init tests" [idTest; incTest]
