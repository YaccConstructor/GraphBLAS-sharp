module GraphBLAS.FSharp.Tests.Host.IO.MtxReader

open System.IO
open Expecto
open GraphBLAS.FSharp.IO

let matrixName = "testMatrix.mtx"

let path =
    Path.Combine [| __SOURCE_DIRECTORY__
                    "Dataset"
                    matrixName |]

let test =
    test "mtxReader test" {
        let matrixReader = MtxReader(path)

        let shape = matrixReader.ReadMatrixShape()

        "Rows count must be the same"
        |> Expect.equal shape.RowCount 2

        "Columns count must be the same"
        |> Expect.equal shape.ColumnCount 3

        "NNZ count must be the same"
        |> Expect.equal shape.NNZ 3

        let matrix = matrixReader.ReadMatrix(int)

        "Matrix row count must be the same"
        |> Expect.equal matrix.RowCount 2

        "Matrix column count must be the same"
        |> Expect.equal matrix.ColumnCount 3

        "Matrix values must be the same"
        |> Expect.sequenceEqual matrix.Values [| 3; 2; 1 |]

        "Matrix columns must be the same"
        |> Expect.sequenceEqual matrix.Columns [| 1; 1; 2 |]

        "Matrix rows must be the same"
        |> Expect.sequenceEqual matrix.Rows [| 0; 1; 1 |]
    }
