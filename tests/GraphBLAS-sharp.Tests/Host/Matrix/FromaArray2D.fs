module GraphBLAS.FSharp.Tests.Host.Matrix.FromArray2D

open Expecto
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Tests

let config = Utils.defaultConfig

let checkPointers isEqual zero array slice counter pointers (matrixValues: 'a []) (matrixIndices: int []) =
    for i in 0 .. counter - 1 do
        let expectedIndices, expectedValues =
            slice array i
            |> Array.mapi (fun index value -> (index, value))
            |> Array.filter (fun (_, value) -> ((<<) not <| isEqual zero) value)
            |> Array.unzip

        let startRowPosition = Array.item i pointers
        let endRowPosition = pointers.[i + 1] - 1

        let actualValues =
            matrixValues.[startRowPosition..endRowPosition]

        let actualIndices =
            matrixIndices.[startRowPosition..endRowPosition]

        "Values must be the same"
        |> Utils.compareArrays isEqual actualValues expectedValues

        "Indices must be the same"
        |> Utils.compareArrays (=) actualIndices expectedIndices

let makeTest isEqual zero createMatrix (array: 'a [,]) =
    let matrix: Matrix<_> = createMatrix (isEqual zero) array

    let arrayRowCount = Array2D.length1 array
    let arrayColumnCount = Array2D.length2 array

    "Row count must be the same"
    |> Expect.equal matrix.RowCount arrayRowCount

    "Column count must be the same"
    |> Expect.equal matrix.ColumnCount arrayColumnCount

    let nonZeroValues =
        array
        |> Seq.cast<'a>
        |> Seq.filter ((<<) not <| isEqual zero)
        |> Seq.toArray

    let checkPointers = checkPointers isEqual zero array

    match matrix with
    | Matrix.CSR matrix ->
        "Values must be the same"
        |> Utils.compareArrays isEqual matrix.Values nonZeroValues

        "Row count invariant"
        |> Expect.isTrue (matrix.RowPointers.Length = matrix.RowCount + 1)

        checkPointers
            (fun (array: 'a [,]) i -> array.[i, *])
            arrayRowCount
            matrix.RowPointers
            matrix.Values
            matrix.ColumnIndices
    | Matrix.COO matrix ->
        "Values must be the same"
        |> Utils.compareArrays isEqual matrix.Values nonZeroValues

        let expectedColumns, expectedRows, expectedValues =
            array
            |> Array2D.mapi (fun rowIndex columnIndex value -> (columnIndex, rowIndex, value))
            |> Seq.cast<int*int*'a>
            |> Seq.filter (fun (_, _, value) -> ((<<) not <| isEqual zero) value)
            |> Seq.toArray
            |> Array.unzip3

        "Values must be the same"
        |> Utils.compareArrays isEqual matrix.Values expectedValues

        "Column indices must be the same"
        |> Utils.compareArrays (=) matrix.Columns expectedColumns

        "Rows indices must be the same"
        |> Utils.compareArrays (=) matrix.Rows expectedRows
    | Matrix.CSC matrix ->
        let expectedValues =
            seq {
                for i in 0 .. arrayColumnCount - 1 do
                    yield! array.[*, i]
            }
            |> Seq.filter ((<<) not <| isEqual zero)
            |> Seq.toArray

        "Values must be the same"
        |> Utils.compareArrays isEqual matrix.Values expectedValues

        "Row count invariant"
        |> Expect.isTrue (matrix.ColumnPointers.Length = matrix.ColumnCount + 1)

        checkPointers
            (fun array i -> array.[*, i])
            arrayColumnCount
            matrix.ColumnPointers
            matrix.Values
            matrix.RowIndices
    | Matrix.LIL matrix ->
        "Rows count must be the same"
        |> Expect.equal matrix.Rows.Length (Array2D.length1 array)

        matrix.Rows
        |> Seq.iteri
            (fun index ->
                function
                | Some actualRow ->
                    let expectedIndices, expectedValues =
                        array.[index, *]
                        |> Array.mapi (fun index value -> (index, value))
                        |> Array.filter (fun (_, value) -> ((<<) not <| isEqual zero) value)
                        |> Array.unzip

                    "Values must be the same"
                    |> Utils.compareArrays isEqual actualRow.Values expectedValues

                    "Indices must be the same"
                    |> Utils.compareArrays (=) actualRow.Indices expectedIndices
                | None ->
                    "No non zero items in row"
                    |> Expect.isFalse (Array.exists ((<<) not <| isEqual zero) array.[index, *]))

let createTest name isEqual zero convert =
    makeTest isEqual zero convert
    |> testPropertyWithConfig config name

let tests =
    [ createTest
        "CSR"
        (=)
        0
        (fun isZero array ->
            Matrix.CSR
            <| Matrix.CSR.FromArray2D(array, isZero))
      createTest
          "COO"
          (=)
          0
          (fun isZero array ->
              Matrix.COO
              <| Matrix.COO.FromArray2D(array, isZero))
      createTest
          "CSC"
          (=)
          0
          (fun isZero array ->
              Matrix.CSC
              <| Matrix.CSC.FromArray2D(array, isZero))
      createTest
          "LIL"
          (=)
          0
          (fun isZero array ->
              Matrix.LIL
              <| Matrix.LIL.FromArray2D(array, isZero)) ]
    |> testList "FromArray2D"
