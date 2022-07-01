namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open Microsoft.FSharp.Quotations

module Matrix =
    let copy (clContext: ClContext) workGroupSize =
        let copy =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize

        let copyData =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO m ->
                let res =
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      Rows = copy processor m.Rows
                      Columns = copy processor m.Columns
                      Values = copyData processor m.Values }

                MatrixCOO res
            | MatrixCSR m ->
                let res =
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      RowPointers = copy processor m.RowPointers
                      Columns = copy processor m.Columns
                      Values = copyData processor m.Values }

                MatrixCSR res

    let toCSR (clContext: ClContext) workGroupSize =
        let toCSR = COOMatrix.toCSR clContext workGroupSize
        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO m -> toCSR processor m |> MatrixCSR
            | MatrixCSR _ -> copy processor matrix

    let toCOO (clContext: ClContext) workGroupSize =
        let toCOO = CSRMatrix.toCOO clContext workGroupSize
        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO _ -> copy processor matrix
            | MatrixCSR m -> toCOO processor m |> MatrixCOO

    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a -> 'a -> 'a>) workGroupSize =
        let COOeWiseAdd =
            COOMatrix.eWiseAdd clContext opAdd workGroupSize

        let CSReWiseAdd =
            CSRMatrix.eWiseAdd clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | MatrixCOO m1, MatrixCOO m2 -> COOeWiseAdd processor m1 m2 |> MatrixCOO
            | MatrixCSR m1, MatrixCSR m2 -> CSReWiseAdd processor m1 m2 |> MatrixCSR
            | _ -> failwith "Matrix formats are not matching"
