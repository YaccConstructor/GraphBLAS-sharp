module GraphBLAS.FSharp.Tests.Backend.Matrix.Intersect

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ArraysExtensions

let config =
    { Utils.defaultConfig with
        arbitrary = [ typeof<Generators.PairOfSparseMatrices> ] }

let workGroupSize = Utils.defaultWorkGroupSize

let context = Context.defaultContext.ClContext
let processor = Context.defaultContext.Queue

let makeTest isZero testFun (leftMatrix: 'a [,], rightMatrix: 'a [,]) =

    let m1 = Matrix.COO.FromArray2D(leftMatrix, isZero)
    let m2 = Matrix.COO.FromArray2D(rightMatrix, isZero)

    if m1.NNZ > 0 && m2.NNZ > 0 then
        let expected =
            let leftIndices =
                (m1.Rows, m1.Columns)
                ||> Array.zip

            let rightIndices =
                (m2.Rows, m2.Columns)
                ||> Array.zip

            Array.init
            <| m1.NNZ
            <| fun i ->
                let index = leftIndices.[i]
                if Array.exists ((=) index) rightIndices then 1 else 0

        let m1 = m1.ToDevice context
        let m2 = m2.ToDevice context

        let actual: ClArray<int> =
            testFun processor ClContextExtensions.HostInterop m1 m2

        let actual = actual.ToHostAndFree processor

        m1.Dispose processor
        m2.Dispose processor

        // Check result
        "Matrices should be equal"
        |> Expect.equal actual expected

let createTest isZero =
    Matrix.COO.Matrix.findKeysIntersection context workGroupSize
    |> makeTest isZero
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest ((=) false)
      createTest ((=) 0)
      createTest ((=) 0uy)
      createTest (Utils.float32IsEqual 0.0f)

      if Utils.isFloat64Available context.ClDevice then
          createTest (Utils.floatIsEqual 0.0) ]
    |> testList "Intersect tests"
