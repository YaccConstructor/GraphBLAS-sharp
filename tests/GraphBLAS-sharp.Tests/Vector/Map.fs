module GraphBLAS.FSharp.Tests.Vector.Map

open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Tests
open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests
open Context
open TestCases
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Quotes

let processor = Context.defaultContext.Queue

let context = Context.defaultContext.ClContext


let config = Utils.defaultConfig

let makeTest<'a> op isEqual zero testFun (array: 'a []) =

    let vector = Vector.Sparse.FromArray(array, isEqual zero)

    if vector.NNZ > 0 then
        let clVector = vector.ToDevice context

        let (clActual: ClVector.Sparse<'a>) =
            testFun processor HostInterop clVector

        let actual = clActual.ToHost processor

        let expectedIndices, expectedValues =
            array
            // apply op
            |> Array.map (fun item ->
                if isEqual zero item then None else op <| Some item)
            // Dense to Sparse
            |> Array.mapi (fun index -> function
                | Some value -> Some (index, value)
                | None -> None)
            |> Array.choose id
            |> Array.unzip

        "Indices must be the same"
        |> Utils.compareArrays (=) actual.Indices expectedIndices

        "Values must be the same"
        |> Utils.compareArrays isEqual actual.Values expectedValues

let createTest<'a when 'a : struct and 'a : equality> isEqual (zero: 'a) (opQ, op) =
    Vector.Sparse.Map.run context Utils.defaultWorkGroupSize opQ
    |> makeTest<'a> op isEqual zero
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int> (=) 0  ]
