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

let context = defaultContext.ClContext
let processor = defaultContext.Queue

let makeTest<'a when 'a: struct> isZero testFun (leftMatrix: 'a [,], rightMatrix: 'a [,]) =

    let m1 =
        Matrix.COO.FromArray2D(leftMatrix, isZero)

    let m2 =
        Matrix.COO.FromArray2D(rightMatrix, isZero)

    if m1.NNZ > 0 && m2.NNZ > 0 then

        let expected =
            let mutable index = 0
            let bitmap = Array.zeroCreate m1.NNZ

            leftMatrix
            |> Array2D.iteri
                (fun row col value ->
                    if row < m2.RowCount
                       && col < m2.ColumnCount
                       && not <| isZero rightMatrix.[row, col]
                       && not <| isZero value then
                        bitmap.[index] <- 1

                    if not <| isZero value then
                        index <- index + 1)

            bitmap

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

let inline createTest<'a when 'a: struct> (isZero: 'a -> bool) =
    Matrix.COO.Matrix.findKeysIntersection context workGroupSize
    |> makeTest<'a> isZero
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest ((=) false)
      createTest ((=) 0)
      createTest ((=) 0uy)
      createTest (Utils.float32IsEqual 0.0f)

      if Utils.isFloat64Available context.ClDevice then
          createTest (Utils.floatIsEqual 0.0) ]
    |> testList "Intersect tests"
