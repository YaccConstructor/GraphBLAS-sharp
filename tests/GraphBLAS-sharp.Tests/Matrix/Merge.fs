module GraphBLAS.FSharp.Tests.Matrix.Merge

open Brahma.FSharp
open Expecto
open Microsoft.FSharp.Collections
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config = Utils.defaultConfig

let checkResult isEqual zero (actual: Matrix.COO<'a>) (leftArray: 'a [,]) (rightArray: 'a [,]) =

    let leftMatrix =
        Matrix.COO.FromArray2D(leftArray, isEqual zero)

    let rightMatrix =
        Matrix.COO.FromArray2D(rightArray, isEqual zero)

    let expectedRows, expectedColumns, expectedValues =
        let leftKeys =
            Seq.zip3 leftMatrix.Rows leftMatrix.Columns leftMatrix.Values

        let rightKeys =
            Seq.zip3 rightMatrix.Rows rightMatrix.Columns rightMatrix.Values

        // right first
        Seq.concat [ rightKeys; leftKeys ]
        |> Seq.sortBy (fun (fstKey, sndKey, _) -> (fstKey, sndKey))
        |> Seq.toArray
        |> Array.unzip3

    "Rows must be the same"
    |> Expect.sequenceEqual actual.Rows expectedRows

    "Columns must be the same"
    |> Expect.sequenceEqual actual.Columns expectedColumns

    "Values must be the same"
    |> Utils.compareArrays isEqual actual.Values expectedValues

let makeTestCOO isEqual zero testFun (leftArray: 'a [,], rightArray: 'a [,]) =

    let leftMatrix =
        Matrix.COO.FromArray2D(leftArray, isEqual zero)

    let rightMatrix =
        Matrix.COO.FromArray2D(rightArray, isEqual zero)

    if leftMatrix.NNZ > 0 && rightMatrix.NNZ > 0 then

        let clLeftMatrix = leftMatrix.ToDevice context

        let clRightMatrix = rightMatrix.ToDevice context

        let ((clRows: ClArray<int>),
             (clColumns: ClArray<int>),
             (clLeftValues: ClArray<'a>),
             (clRightValues: ClArray<'a>),
             (clIsLeft: ClArray<int>)) =
            testFun processor clLeftMatrix clRightMatrix

        clLeftMatrix.Dispose processor
        clRightMatrix.Dispose processor

        let leftValues = clLeftValues.ToHostAndFree processor
        let rightValues = clRightValues.ToHostAndFree processor
        let isLeft = clIsLeft.ToHostAndFree processor

        let actualValues =
            Array.map3
                (fun leftValue rightValue isLeft ->
                    if isLeft = 1 then
                        leftValue
                    else
                        rightValue)
            <| leftValues
            <| rightValues
            <| isLeft

        let actual =
            { Matrix.COO.RowCount = leftMatrix.RowCount
              Matrix.COO.ColumnCount = leftMatrix.ColumnCount
              Matrix.COO.Rows = clRows.ToHostAndFree processor
              Matrix.COO.Columns = clColumns.ToHostAndFree processor
              Matrix.COO.Values = actualValues }

        checkResult isEqual zero actual leftArray rightArray

let createTestCOO isEqual (zero: 'a) =
    Matrix.COO.Merge.run context Utils.defaultWorkGroupSize
    |> makeTestCOO isEqual zero
    |> testPropertyWithConfig config $"test on {typeof<'a>}"

let testsCOO =
    [ createTestCOO (=) 0

      if Utils.isFloat64Available context.ClDevice then
          createTestCOO (=) 0.0

      createTestCOO (=) 0.0f
      createTestCOO (=) false ]
    |> testList "COO"

let makeTestCSR isEqual zero testFun (leftArray: 'a [,], rightArray: 'a [,]) =
    let leftMatrix =
        Matrix.CSR.FromArray2D(leftArray, isEqual zero)

    let rightMatrix =
        Matrix.CSR.FromArray2D(rightArray, isEqual zero)

    if leftMatrix.NNZ > 0 && rightMatrix.NNZ > 0 then

        let clLeftMatrix = leftMatrix.ToDevice context

        let clRightMatrix = rightMatrix.ToDevice context

        let ((clRows: ClArray<int>),
             (clColumns: ClArray<int>),
             (clLeftValues: ClArray<'a>),
             (clRightValues: ClArray<'a>),
             (clIsEndOfRow: ClArray<int>),
             (clIsLeft: ClArray<int>)) =
            testFun processor clLeftMatrix clRightMatrix

        clLeftMatrix.Dispose processor
        clRightMatrix.Dispose processor

        let leftValues = clLeftValues.ToHostAndFree processor
        let rightValues = clRightValues.ToHostAndFree processor
        clIsEndOfRow.Free processor
        let isLeft = clIsLeft.ToHostAndFree processor

        let actualValues =
            Array.map3
                (fun leftValue rightValue isLeft ->
                    if isLeft = 1 then
                        leftValue
                    else
                        rightValue)
            <| leftValues
            <| rightValues
            <| isLeft

        let actual =
            { Matrix.COO.RowCount = leftMatrix.RowCount
              Matrix.COO.ColumnCount = leftMatrix.ColumnCount
              Matrix.COO.Rows = clRows.ToHostAndFree processor
              Matrix.COO.Columns = clColumns.ToHostAndFree processor
              Matrix.COO.Values = actualValues }

        checkResult isEqual zero actual leftArray rightArray

let createTestCSR isEqual (zero: 'a) =
    Matrix.CSR.Merge.run context Utils.defaultWorkGroupSize
    |> makeTestCSR isEqual zero
    |> testPropertyWithConfig { config with endSize = 10 } $"test on {typeof<'a>}"

let testsCSR =
    [ createTestCSR (=) 0

      if Utils.isFloat64Available context.ClDevice then
          createTestCSR (=) 0.0

      createTestCSR (=) 0.0f
      createTestCSR (=) false ]
    |> testList "CSR"

let allTests =
    [ testsCSR; testsCOO ] |> testList "Merge"
