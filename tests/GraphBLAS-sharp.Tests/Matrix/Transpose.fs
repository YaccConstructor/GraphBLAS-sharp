module GraphBLAS.FSharp.Tests.Backend.Matrix.Transpose

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.TestCases
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Transpose.Tests"

let config = Utils.defaultConfig

let wgSize = Utils.defaultWorkGroupSize

let getCorrectnessTestName case datatype =
    $"Correctness on %s{datatype}, %A{case.Format}, %A{case.TestContext}"

let checkResult areEqual zero actual (expected2D: 'a [,]) =
    match actual with
    | Matrix.COO actual ->
        let expected =
            Matrix.COO.FromArray2D(expected2D, areEqual zero)

        "The number of rows should be the same"
        |> Expect.equal actual.RowCount expected.RowCount

        "The number of columns should be the same"
        |> Expect.equal actual.ColumnCount expected.ColumnCount

        "Row arrays should be equal"
        |> Utils.compareArrays (=) actual.Rows expected.Rows

        "Column arrays should be equal"
        |> Utils.compareArrays (=) actual.Columns expected.Columns

        "Value arrays should be equal"
        |> Utils.compareArrays areEqual actual.Values expected.Values
    | Matrix.CSR actual ->
        let expected =
            Matrix.CSR.FromArray2D(expected2D, areEqual zero)

        "The number of rows should be the same"
        |> Expect.equal actual.RowCount expected.RowCount

        "The number of columns should be the same"
        |> Expect.equal actual.ColumnCount expected.ColumnCount

        "Row pointer arrays should be equal"
        |> Utils.compareArrays (=) actual.RowPointers expected.RowPointers

        "Column arrays should be equal"
        |> Utils.compareArrays (=) actual.ColumnIndices expected.ColumnIndices

        "Value arrays should be equal"
        |> Utils.compareArrays areEqual actual.Values expected.Values
    | Matrix.CSC actual ->
        let expected =
            Matrix.CSC.FromArray2D(expected2D, areEqual zero)

        "The number of rows should be the same"
        |> Expect.equal actual.RowCount expected.RowCount

        "The number of columns should be the same"
        |> Expect.equal actual.ColumnCount expected.ColumnCount

        "Row arrays should be equal"
        |> Utils.compareArrays (=) actual.RowIndices expected.RowIndices

        "Column pointer arrays should be equal"
        |> Utils.compareArrays (=) actual.ColumnPointers expected.ColumnPointers

        "Value arrays should be equal"
        |> Utils.compareArrays areEqual actual.Values expected.Values

let makeTestRegular context q transposeFun hostTranspose isEqual zero case (array: 'a [,]) =
    let mtx =
        Utils.createMatrixFromArray2D case.Format array (isEqual zero)

    if mtx.NNZ > 0 then
        let actual =
            let m = mtx.ToDevice context
            let (mT: ClMatrix<'a>) = transposeFun q HostInterop m
            let res = mT.ToHost q
            m.Dispose q
            mT.Dispose q
            res

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" $"%A{actual}"
        )

        let expected2D = hostTranspose array

        checkResult isEqual zero actual expected2D

let createTest<'a when 'a: equality and 'a: struct> case (zero: 'a) isEqual =
    let context = case.TestContext.ClContext
    let q = case.TestContext.Queue

    let transposeFun = Matrix.transpose context wgSize

    let twiceTranspose processor allocationFlag matrix =
        transposeFun processor allocationFlag matrix
        |> transposeFun processor allocationFlag

    [ case
      |> makeTestRegular context q transposeFun Utils.transpose2DArray isEqual zero
      |> testPropertyWithConfig config "single transpose"

      case
      |> makeTestRegular context q twiceTranspose id isEqual zero
      |> testPropertyWithConfig config "twice transpose" ]

    |> testList (getCorrectnessTestName case $"{typeof<'a>}")

let testFixtures case =
    let context = case.TestContext.ClContext
    let q = case.TestContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    [ createTest<int> case 0 (=)

      if Utils.isFloat64Available context.ClDevice then
          createTest<float> case 0.0 Utils.floatIsEqual

      createTest<float32> case 0.0f Utils.float32IsEqual
      createTest<byte> case 0uy (=)
      createTest<bool> case false (=) ]

let tests =
    operationGPUTests "Matrix.Transpose tests" testFixtures
