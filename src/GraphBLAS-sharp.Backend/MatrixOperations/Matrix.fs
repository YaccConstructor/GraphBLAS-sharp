namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common

module Matrix =
    let copy (clContext: ClContext) =
        let copy = ClArray.copy clContext
        let copyData = ClArray.copy clContext

        fun (processor: MailboxProcessor<_>) workGroupSize (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO m ->
                let res =
                    {
                        Context = clContext
                        RowCount = m.RowCount
                        ColumnCount = m.ColumnCount
                        Rows = copy processor workGroupSize m.Rows
                        Columns = copy processor workGroupSize m.Columns
                        Values = copyData processor workGroupSize m.Values
                    }

                ClMatrixCOO res

            | ClMatrixCSR m ->
                let res =
                    {
                        Context = clContext
                        RowCount = m.RowCount
                        ColumnCount = m.ColumnCount
                        RowPointers = copy processor workGroupSize m.RowPointers
                        Columns = copy processor workGroupSize m.Columns
                        Values = copyData processor workGroupSize m.Values
                    }

                ClMatrixCSR res

    let toCSR (clContext: ClContext) workGroupSize =
        let toCSR = COOMatrix.toCSR clContext workGroupSize
        let copy = copy clContext

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO m -> toCSR processor m |> ClMatrixCSR
            | ClMatrixCSR _ -> copy processor workGroupSize matrix

    let toCOO (clContext: ClContext) workGroupSize =
        let toCOO = CSRMatrix.toCOO clContext workGroupSize
        let copy = copy clContext

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO _ -> copy processor workGroupSize matrix
            | ClMatrixCSR m -> toCOO processor m |> ClMatrixCOO

    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =
        let COOeWiseAdd = COOMatrix.eWiseAdd clContext opAdd workGroupSize
        let CSReWiseAdd = CSRMatrix.eWiseAdd clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrixCOO m1, ClMatrixCOO m2 -> COOeWiseAdd processor m1 m2 |> ClMatrixCOO
            | ClMatrixCSR m1, ClMatrixCSR m2 -> CSReWiseAdd processor m1 m2 |> ClMatrixCSR
            | _ -> failwith "Matrix formats are not matching"

    let eWiseAddAtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =
        let COOeWiseAdd = COOMatrix.eWiseAddAtLeastOne clContext opAdd workGroupSize
        let CSReWiseAdd = CSRMatrix.eWiseAddAtLeastOne clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrixCOO m1, ClMatrixCOO m2 -> COOeWiseAdd processor m1 m2 |> ClMatrixCOO
            | ClMatrixCSR m1, ClMatrixCSR m2 -> CSReWiseAdd processor m1 m2 |> ClMatrixCSR
            | _ -> failwith "Matrix formats are not matching"
