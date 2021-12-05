namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations

module Matrix =
    let toCSR (clContext: ClContext) workGroupSize processor matrix =
        let toCSR =
            COOMatrix.toCSR clContext workGroupSize processor

        match matrix with
        | MatrixCOO m -> toCSR m |> MatrixCSR
        | MatrixCSR _ -> matrix

    let toCOO (clContext: ClContext) workGroupSize processor matrix =
        let toCOO =
            CSRMatrix.toCOO clContext processor workGroupSize

        match matrix with
        | MatrixCOO _ -> matrix
        | MatrixCSR m -> toCOO m |> MatrixCOO

    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a -> 'a -> 'a>) workGroupSize processor matrix1 matrix2 =
        let COOeWiseAdd =
            COOMatrix.eWiseAdd clContext opAdd workGroupSize processor

        let CSReWiseAdd =
            CSRMatrix.eWiseAdd clContext opAdd workGroupSize processor

        match matrix1, matrix2 with
        | MatrixCOO m1, MatrixCOO m2 -> COOeWiseAdd m1 m2 |> MatrixCOO
        | MatrixCSR m1, MatrixCSR m2 -> CSReWiseAdd m1 m2 |> MatrixCSR
        | _ -> failwith "Matrix formats are not matching"
