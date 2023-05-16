module GraphBLAS.FSharp.Tests.Backend.Matrix.SubRows

open Expecto
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Objects.Matrix

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.Matrix.Sub> ] }

let makeTest isEqual zero testFun (array: 'a [,], sourceRow, count) =

    let matrix =
        Matrix.CSR.FromArray2D(array, isEqual zero)

    if matrix.NNZ > 0 then

        let clMatrix = matrix.ToDevice context

        let clActual: ClMatrix.COO<'a> =
            testFun processor HostInterop sourceRow count clMatrix

        let actual = clActual.ToHostAndFree processor

        let expected =
            array
            |> Array2D.mapi (fun rowIndex columnIndex value -> (value, rowIndex, columnIndex))
            |> fun array -> array.[sourceRow..sourceRow + count - 1, *]
            |> Seq.cast<'a * int * int>
            |> Seq.filter (fun (value, _, _) -> (not <| isEqual zero value))
            |> Seq.toArray
            |> Array.unzip3
            |> fun (values, rows, columns) ->
                { RowCount = Array2D.length1 array
                  ColumnCount = Array2D.length2 array
                  Rows = rows
                  Columns = columns
                  Values = values }

        Utils.compareCOOMatrix isEqual actual expected

let createTest isEqual (zero: 'a) =
    CSR.Matrix.subRows context Utils.defaultWorkGroupSize
    |> makeTest isEqual zero
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let tests =
    [ createTest (=) 0

      if Utils.isFloat64Available context.ClDevice then
          createTest (=) 0.0

      createTest (=) 0.0f
      createTest (=) false ]
    |> testList "Blit"
