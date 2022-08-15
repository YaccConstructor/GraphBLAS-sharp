namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common

module Matrix =
    let copy (clContext: ClContext) =
        let copy =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext

        let copyData =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext

        fun (processor: MailboxProcessor<_>) workGroupSize (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO m ->
                let res =
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      Rows = copy processor workGroupSize m.Rows
                      Columns = copy processor workGroupSize m.Columns
                      Values = copyData processor workGroupSize m.Values }

                MatrixCOO res
            | MatrixCSR m ->
                let res =
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      RowPointers = copy processor workGroupSize m.RowPointers
                      Columns = copy processor workGroupSize m.Columns
                      Values = copyData processor workGroupSize m.Values }

                MatrixCSR res

    let toCSR (clContext: ClContext) workGroupSize =
        let toCSR = COOMatrix.toCSR clContext workGroupSize
        let copy = copy clContext

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO m -> toCSR processor m |> MatrixCSR
            | MatrixCSR _ -> copy processor workGroupSize matrix

    let toCOO (clContext: ClContext) workGroupSize =
        let toCOO = CSRMatrix.toCOO clContext workGroupSize
        let copy = copy clContext

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO _ -> copy processor workGroupSize matrix
            | MatrixCSR m -> toCOO processor m |> MatrixCOO

    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =
        let COOeWiseAdd =
            COOMatrix.eWiseAdd clContext opAdd workGroupSize

        let CSReWiseAdd =
            EWise.eWiseAdd clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | MatrixCOO m1, MatrixCOO m2 -> COOeWiseAdd processor m1 m2 |> MatrixCOO
            | MatrixCSR m1, MatrixCSR m2 -> CSReWiseAdd processor m1 m2 |> MatrixCSR
            | _ -> failwith "Matrix formats are not matching"

    let eWiseAddAtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =
        let COOeWiseAdd =
            COOMatrix.eWiseAddAtLeastOne clContext opAdd workGroupSize

        let CSReWiseAdd =
            CSRMatrix.eWiseAddAtLeastOne clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | MatrixCOO m1, MatrixCOO m2 -> COOeWiseAdd processor m1 m2 |> MatrixCOO
            | MatrixCSR m1, MatrixCSR m2 -> CSReWiseAdd processor m1 m2 |> MatrixCSR
            | _ -> failwith "Matrix formats are not matching"
