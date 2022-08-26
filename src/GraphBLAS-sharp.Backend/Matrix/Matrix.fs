namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common

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

    let toCSRInplace (clContext: ClContext) workGroupSize =
        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO m -> toCSRInplace processor m |> MatrixCSR
            | MatrixCSR _ -> matrix

    let toCOO (clContext: ClContext) workGroupSize =
        let toCOO = CSRMatrix.toCOO clContext workGroupSize
        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO _ -> copy processor matrix
            | MatrixCSR m -> toCOO processor m |> MatrixCOO

    let toCOOInplace (clContext: ClContext) workGroupSize =
        let toCOOInplace =
            CSRMatrix.toCOOInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO _ -> matrix
            | MatrixCSR m -> toCOOInplace processor m |> MatrixCOO

    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =
        let COOeWiseAdd =
            COOMatrix.eWiseAdd clContext opAdd workGroupSize

        let CSReWiseAdd =
            CSRMatrix.eWiseAdd clContext opAdd workGroupSize

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


    let transposeInplace (clContext: ClContext) workGroupSize =
        let COOtransposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        let CSRtransposeInplace =
            CSRMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix ->
            match matrix with
            | MatrixCOO m -> COOtransposeInplace processor m |> MatrixCOO
            | MatrixCSR m -> CSRtransposeInplace processor m |> MatrixCSR


    let transpose (clContext: ClContext) workGroupSize =
        let COOtranspose =
            COOMatrix.transpose clContext workGroupSize

        let CSRtranspose =
            CSRMatrix.transpose clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix ->
            match matrix with
            | MatrixCOO m -> COOtranspose processor m |> MatrixCOO
            | MatrixCSR m -> CSRtranspose processor m |> MatrixCSR
