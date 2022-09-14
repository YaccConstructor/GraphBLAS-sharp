namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common

module Matrix =
    let copy (clContext: ClContext) workGroupSize =
        let copy = ClArray.copy clContext workGroupSize
        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO m ->
                ClMatrixCOO {
                    Context = clContext
                    RowCount = m.RowCount
                    ColumnCount = m.ColumnCount
                    Rows = copy processor m.Rows
                    Columns = copy processor m.Columns
                    Values = copyData processor m.Values
                }

            | ClMatrixCSR m ->
                ClMatrixCSR {
                    Context = clContext
                    RowCount = m.RowCount
                    ColumnCount = m.ColumnCount
                    RowPointers = copy processor m.RowPointers
                    Columns = copy processor m.Columns
                    Values = copyData processor m.Values
                }

    /// <summary>
    /// Creates a new matrix, represented in CSR format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSR (clContext: ClContext) workGroupSize =
        let toCSR = COOMatrix.toCSR clContext workGroupSize
        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO m -> toCSR processor m |> ClMatrixCSR
            | ClMatrixCSR _ -> copy processor matrix

    /// <summary>
    /// Returns the matrix, represented in CSR format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSRInplace (clContext: ClContext) workGroupSize =
        let toCSRInplace = COOMatrix.toCSRInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO m -> toCSRInplace processor m |> ClMatrixCSR
            | ClMatrixCSR _ -> matrix

    /// <summary>
    /// Creates a new matrix, represented in COO format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCOO (clContext: ClContext) workGroupSize =
        let toCOO = CSRMatrix.toCOO clContext workGroupSize
        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO _ -> copy processor matrix
            | ClMatrixCSR m -> toCOO processor m |> ClMatrixCOO

    /// <summary>
    /// Returns the matrix, represented in COO format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCOOInplace (clContext: ClContext) workGroupSize =
        let toCOOInplace = CSRMatrix.toCOOInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO _ -> matrix
            | ClMatrixCSR m -> toCOOInplace processor m |> ClMatrixCOO

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

    /// <summary>
    /// Transposes the given matrix and returns result. The storage format is preserved.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let transposeInplace (clContext: ClContext) workGroupSize =
        let COOtransposeInplace = COOMatrix.transposeInplace clContext workGroupSize
        let CSRtransposeInplace = CSRMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix ->
            match matrix with
            | ClMatrixCOO m -> COOtransposeInplace processor m |> ClMatrixCOO
            | ClMatrixCSR m -> CSRtransposeInplace processor m |> ClMatrixCSR

    /// <summary>
    /// Transposes the given matrix and returns result as a new matrix. The storage format is preserved.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let transpose (clContext: ClContext) workGroupSize =
        let COOtranspose = COOMatrix.transpose clContext workGroupSize
        let CSRtranspose = CSRMatrix.transpose clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix ->
            match matrix with
            | ClMatrixCOO m -> COOtranspose processor m |> ClMatrixCOO
            | ClMatrixCSR m -> CSRtranspose processor m |> ClMatrixCSR
