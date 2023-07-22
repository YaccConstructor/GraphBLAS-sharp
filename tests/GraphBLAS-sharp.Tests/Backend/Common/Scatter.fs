module GraphBLAS.FSharp.Tests.Backend.Common.Scatter

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Objects.ArraysExtensions

let logger = Log.create "Scatter.Tests"

let context = defaultContext.ClContext

let config = Utils.defaultConfig

let wgSize = Utils.defaultWorkGroupSize

let q = defaultContext.Queue

let makeTest<'a when 'a: equality> hostScatter scatter (array: (int * 'a) []) (result: 'a []) =
    if array.Length > 0 then
        let positions, values = Array.sortBy fst array |> Array.unzip

        let expected =
            Array.copy result |> hostScatter positions values

        let actual =
            let clPositions = context.CreateClArray positions
            let clValues = context.CreateClArray values
            let clResult = context.CreateClArray result

            scatter q clPositions clValues clResult

            clValues.Free q
            clPositions.Free q
            clResult.ToHostAndFree q

        $"Arrays should be equal."
        |> Utils.compareArrays (=) actual expected

let testFixturesLast<'a when 'a: equality> =
    Common.Scatter.lastOccurrence context wgSize
    |> makeTest<'a> HostPrimitives.scatterLastOccurrence
    |> testPropertyWithConfig config $"Correctness on %A{typeof<'a>}"

let testFixturesFirst<'a when 'a: equality> =
    Common.Scatter.firstOccurrence context wgSize
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

    testList "ones occurrence" [ first; last ]

let makeTestInit<'a when 'a: equality> hostScatter valueMap scatter (positions: int []) (result: 'a []) =
    if positions.Length > 0 then

        let values = Array.init positions.Length valueMap
        let positions = Array.sort positions

        let expected =
            Array.copy result |> hostScatter positions values

        let clPositions = context.CreateClArray positions
        let clResult = context.CreateClArray result

        scatter q clPositions clResult

        clPositions.Free q
        let actual = clResult.ToHostAndFree q

        $"Arrays should be equal."
        |> Utils.compareArrays (=) actual expected

let createInitTest clScatter hostScatter name valuesMap valuesMapQ =
    let scatter =
        clScatter valuesMapQ context Utils.defaultWorkGroupSize

    makeTestInit<'a> hostScatter valuesMap scatter
    |> testPropertyWithConfig config name

let initTests =
    q.Error.Add(fun e -> failwithf $"%A{e}")

    let inc = ((+) 1)

    let firstOccurrence =
        [ createInitTest Common.Scatter.initFirstOccurrence HostPrimitives.scatterFirstOccurrence "id" id Map.id
          createInitTest Common.Scatter.initFirstOccurrence HostPrimitives.scatterFirstOccurrence "inc" inc Map.inc ]
        |> testList "first occurrence"

    let lastOccurrence =
        [ createInitTest Common.Scatter.initLastOccurrence HostPrimitives.scatterLastOccurrence "id" id Map.id
          createInitTest Common.Scatter.initLastOccurrence HostPrimitives.scatterLastOccurrence "inc" inc Map.inc ]
        |> testList "last occurrence"

    testList "init" [ firstOccurrence; lastOccurrence ]

let allTests = testList "Scatter" [ tests; initTests ]
