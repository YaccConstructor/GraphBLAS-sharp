namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations

module Matrix =
    let toCSR (clContext: ClContext) workGroupSize =
        let toCSR = COOMatrix.toCSR clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO m -> toCSR processor m |> MatrixCSR
            | MatrixCSR _ -> matrix

    let toCOO (clContext: ClContext) workGroupSize =
        let toCOO = CSRMatrix.toCOO clContext

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO _ -> matrix
            | MatrixCSR m -> toCOO processor workGroupSize m |> MatrixCOO

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
