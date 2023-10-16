module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Exists

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open Context
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClCellExtensions
open GraphBLAS.FSharp.Backend.Quotes

let logger =
    Log.create "ClArray.containsNonZero.Tests"

let context = defaultContext.ClContext

let q = defaultContext.Queue

let config = Utils.defaultConfig

let wgSize = Utils.defaultWorkGroupSize

let correctnessGenericTest<'a when 'a: struct and 'a: equality> isZero exists (array: 'a []) =

    if array.Length > 0 then
        let vector =
            Utils.createVectorFromArray Dense array isZero

        let result =
            match vector.ToDevice context with
            | ClVector.Dense clArray -> (exists q clArray: ClCell<_>).ToHostAndFree q
            | _ -> failwith "Unsupported vector format"

        $"The results should be the same, vector : {vector}"
        |> Expect.equal result (Array.exists (not << isZero) array)

let createTest<'a when 'a: struct and 'a: equality> isEqual zero =
    let exists =
        ClArray.exists Predicates.isSome context wgSize

    [ correctnessGenericTest<'a> (isEqual zero) exists
      |> testPropertyWithConfig config "FSCheck data"

      correctnessGenericTest<'a> (isEqual zero) exists (Array.create 1000 zero)
      |> testPropertyWithConfig config "Zeros" ]
    |> testList $"Correctness on %A{typeof<'a>}"

let testFixtures =
    [ createTest<int> (=) 0
      createTest<byte> (=) 0uy

      if Utils.isFloat64Available context.ClDevice then
          createTest Utils.floatIsEqual 0.0

      createTest<float32> Utils.float32IsEqual 0.0f
      createTest<bool> (=) false ]

let tests =
    testList "Common.ClArray.exists tests" testFixtures
