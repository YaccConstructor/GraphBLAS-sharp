module GraphBLAS.FSharp.Tests.Matrix.ByRows

open Expecto
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ClVectorExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config = Utils.defaultConfig

let makeTest<'a when 'a: struct> isEqual zero testFun (array: 'a [,]) =

    let matrix =
        Matrix.CSR.FromArray2D(array, isEqual zero)

    if matrix.NNZ > 0 then

        let clMatrix = matrix.ToDevice context

        let rows = testFun processor HostInterop clMatrix

        "Rows count must be the same"
        |> Expect.equal (Seq.length rows) (Array2D.length1 array)

        rows
        |> Seq.iteri
            (fun index ->
                function
                | Some (actualRow: ClVector.Sparse<_>) ->
                    let expectedRow =
                        Vector.Sparse.FromArray(array.[index, *], (isEqual zero))

                    let actualHost = actualRow.ToHost processor

                    Utils.compareSparseVectors isEqual actualHost expectedRow
                | None ->
                    "Expected row must be None"
                    |> Expect.isFalse (Array.exists ((<<) not <| isEqual zero) array.[index, *]))

let createTest isEqual (zero: 'a) =
    CSR.Matrix.byRows context Utils.defaultWorkGroupSize
    |> makeTest<'a> isEqual zero
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let tests =
    [ createTest (=) 0

      if Utils.isFloat64Available context.ClDevice then
          createTest Utils.floatIsEqual 0.0

      createTest Utils.float32IsEqual 0.0f
      createTest (=) false ]
    |> testList "CSR byRows"
