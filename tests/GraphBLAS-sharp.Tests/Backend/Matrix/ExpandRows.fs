module GraphBLAS.FSharp.Tests.Backend.Matrix.ExpandRows

open Expecto
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects.ClContextExtensions
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config = Utils.defaultConfig

let makeTest isZero testFun (array: 'a [,]) =

    let matrix = Matrix.CSR.FromArray2D(array, isZero)

    if matrix.NNZ > 0 then

        let clMatrix = matrix.ToDevice context

        let (clRows: ClArray<int>) = testFun processor HostInterop clMatrix

        let actual = clRows.ToHostAndFree processor

        let expected =
            Matrix.COO.FromArray2D(array, isZero).Rows

        "Result must be the same"
        |> Expect.sequenceEqual actual expected

let createTest (isZero: 'a -> bool) =
    CSR.Matrix.expandRowPointers context Utils.defaultWorkGroupSize
    |> makeTest isZero
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let tests =
    [ createTest ((=) 0)

      if Utils.isFloat64Available context.ClDevice then
          createTest ((=) 0.0)

      createTest ((=) 0.0f)
      createTest ((=) false) ]
    |> testList "Expand rows"
