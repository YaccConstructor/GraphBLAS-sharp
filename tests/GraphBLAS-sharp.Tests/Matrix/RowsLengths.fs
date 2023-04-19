module GraphBLAS.FSharp.Tests.Backend.Matrix.RowsLengths

open Expecto
open Microsoft.FSharp.Collections
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects.ClContext

let processor = Context.defaultContext.Queue

let context = Context.defaultContext.ClContext

let config = Utils.defaultConfig

let makeTest isZero testFun (array: 'a [,]) =

    let matrix = Matrix.CSR.FromArray2D(array, isZero)

    if matrix.NNZ > 0 then

        let clMatrix = matrix.ToDevice context
        let (clActual: ClArray<int>) = testFun processor HostInterop clMatrix

        clMatrix.Dispose processor
        let actual = clActual.ToHostAndFree processor

        let expected =
            matrix.RowPointers
            |> Array.pairwise
            |> Array.map (fun (fst, snd) -> snd - fst)

        "Results must be the same"
        |> Utils.compareArrays (=) actual expected

let createTest<'a when 'a : struct> (isZero: 'a -> bool) =
     CSR.Matrix.getRowsLength context Utils.defaultWorkGroupSize
    |> makeTest isZero
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest<int> <| (=) 0

      if Utils.isFloat64Available context.ClDevice then
          createTest<float> <| Utils.floatIsEqual 0.0

      createTest<float32> <| Utils.float32IsEqual 0.0f
      createTest<bool> <| (=) false ]
    |> testList "CSR.RowsLengths"
