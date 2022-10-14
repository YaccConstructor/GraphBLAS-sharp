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
            | MatrixCSC m ->
                let res =
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      Rows = copy processor m.Rows
                      ColumnPointers = copy processor m.ColumnPointers
                      Values = copyData processor m.Values }

                MatrixCSC res

    /// <summary>
    /// Creates a new matrix, represented in CSR format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSR (clContext: ClContext) workGroupSize =
        let toCSR = COOMatrix.toCSR clContext workGroupSize
        let copy = copy clContext workGroupSize

        let transpose =
            CSRMatrix.transpose clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO m -> toCSR processor m |> MatrixCSR
            | MatrixCSR _ -> copy processor matrix
            | MatrixCSC m ->
                let csrT =
                    { Context = m.Context
                      RowCount = m.ColumnCount
                      ColumnCount = m.RowCount
                      RowPointers = m.ColumnPointers
                      Columns = m.Rows
                      Values = m.Values }

                transpose processor csrT |> MatrixCSR

    /// <summary>
    /// Returns the matrix, represented in CSR format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSRInplace (clContext: ClContext) workGroupSize =
        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        let transposeInplace =
            CSRMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO m -> toCSRInplace processor m |> MatrixCSR
            | MatrixCSR _ -> matrix
            | MatrixCSC m ->
                let csrT =
                    { Context = m.Context
                      RowCount = m.ColumnCount
                      ColumnCount = m.RowCount
                      RowPointers = m.ColumnPointers
                      Columns = m.Rows
                      Values = m.Values }

                transposeInplace processor csrT |> MatrixCSR

    /// <summary>
    /// Creates a new matrix, represented in COO format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCOO (clContext: ClContext) workGroupSize =
        let toCOO = CSRMatrix.toCOO clContext workGroupSize
        let copy = copy clContext workGroupSize

        let transposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO _ -> copy processor matrix
            | MatrixCSR m -> toCOO processor m |> MatrixCOO
            | MatrixCSC m ->
                let csrT =
                    { Context = m.Context
                      RowCount = m.ColumnCount
                      ColumnCount = m.RowCount
                      RowPointers = m.ColumnPointers
                      Columns = m.Rows
                      Values = m.Values }

                let cooT = toCOO processor csrT
                transposeInplace processor cooT |> MatrixCOO

    /// <summary>
    /// Returns the matrix, represented in COO format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCOOInplace (clContext: ClContext) workGroupSize =
        let toCOOInplace =
            CSRMatrix.toCOOInplace clContext workGroupSize

        let transposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCOO _ -> matrix
            | MatrixCSR m -> toCOOInplace processor m |> MatrixCOO
            | MatrixCSC m ->
                let csrT =
                    { Context = m.Context
                      RowCount = m.ColumnCount
                      ColumnCount = m.RowCount
                      RowPointers = m.ColumnPointers
                      Columns = m.Rows
                      Values = m.Values }

                let cooT = toCOOInplace processor csrT
                transposeInplace processor cooT |> MatrixCOO

    /// <summary>
    /// Creates a new matrix, represented in CSC format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSC (clContext: ClContext) workGroupSize =
        let toCSR = COOMatrix.toCSR clContext workGroupSize
        let copy = copy clContext workGroupSize

        let transposeCSR =
            CSRMatrix.transpose clContext workGroupSize

        let transposeCOO =
            COOMatrix.transpose clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCSC _ -> copy processor matrix
            | MatrixCSR m ->
                let csrT = transposeCSR processor m

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> MatrixCSC
            | MatrixCOO m ->
                let cooT = transposeCOO processor m
                let csrT = toCSR processor cooT

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> MatrixCSC

    /// <summary>
    /// Returns the matrix, represented in CSC format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSCInplace (clContext: ClContext) workGroupSize =
        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize

        let transposeCSRInplace =
            CSRMatrix.transposeInplace clContext workGroupSize

        let transposeCOOInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: Matrix<'a>) ->
            match matrix with
            | MatrixCSC _ -> matrix
            | MatrixCSR m ->
                let csrT = transposeCSRInplace processor m

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> MatrixCSC
            | MatrixCOO m ->
                let cooT = transposeCOOInplace processor m
                let csrT = toCSRInplace processor cooT

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> MatrixCSC

    let eWiseAdd (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =
        let COOeWiseAdd =
            COOMatrix.eWiseAdd clContext opAdd workGroupSize

        let CSReWiseAdd =
            CSRMatrix.eWiseAdd clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | MatrixCOO m1, MatrixCOO m2 -> COOeWiseAdd processor m1 m2 |> MatrixCOO
            | MatrixCSR m1, MatrixCSR m2 -> CSReWiseAdd processor m1 m2 |> MatrixCSR
            | MatrixCSC m1, MatrixCSC m2 ->
                let csrT1 =
                    { Context = m1.Context
                      RowCount = m1.ColumnCount
                      ColumnCount = m1.RowCount
                      RowPointers = m1.ColumnPointers
                      Columns = m1.Rows
                      Values = m1.Values }

                let csrT2 =
                    { Context = m2.Context
                      RowCount = m2.ColumnCount
                      ColumnCount = m2.RowCount
                      RowPointers = m2.ColumnPointers
                      Columns = m2.Rows
                      Values = m2.Values }

                let resT = CSReWiseAdd processor csrT1 csrT2

                { Context = resT.Context
                  RowCount = resT.ColumnCount
                  ColumnCount = resT.RowCount
                  Rows = resT.Columns
                  ColumnPointers = resT.RowPointers
                  Values = resT.Values }
                |> MatrixCSC
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
            | MatrixCSC m1, MatrixCSC m2 ->
                let csrT1 =
                    { Context = m1.Context
                      RowCount = m1.ColumnCount
                      ColumnCount = m1.RowCount
                      RowPointers = m1.ColumnPointers
                      Columns = m1.Rows
                      Values = m1.Values }

                let csrT2 =
                    { Context = m2.Context
                      RowCount = m2.ColumnCount
                      ColumnCount = m2.RowCount
                      RowPointers = m2.ColumnPointers
                      Columns = m2.Rows
                      Values = m2.Values }

                let resT = CSReWiseAdd processor csrT1 csrT2

                { Context = resT.Context
                  RowCount = resT.ColumnCount
                  ColumnCount = resT.RowCount
                  Rows = resT.Columns
                  ColumnPointers = resT.RowPointers
                  Values = resT.Values }
                |> MatrixCSC
            | _ -> failwith "Matrix formats are not matching"

    /// <summary>
    /// Transposes the given matrix and returns result.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// The storage format is not guaranteed to be preserved.
    /// </summary>
    /// <remarks>
    /// The format changes according to the following:
    /// * COO -> COO
    /// * CSR -> CSC
    /// * CSC -> CSR
    /// </remarks>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let transposeInplace (clContext: ClContext) workGroupSize =
        let COOtransposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix ->
            match matrix with
            | MatrixCOO m -> COOtransposeInplace processor m |> MatrixCOO
            | MatrixCSR m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  Rows = m.Columns
                  ColumnPointers = m.RowPointers
                  Values = m.Values }
                |> MatrixCSC
            | MatrixCSC m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  RowPointers = m.ColumnPointers
                  Columns = m.Rows
                  Values = m.Values }
                |> MatrixCSR

    /// <summary>
    /// Transposes the given matrix and returns result as a new matrix.
    /// The storage format is not guaranteed to be preserved.
    /// </summary>
    /// <remarks>
    /// The format changes according to the following:
    /// * COO -> COO
    /// * CSR -> CSC
    /// * CSC -> CSR
    /// </remarks>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let transpose (clContext: ClContext) workGroupSize =
        let COOtranspose =
            COOMatrix.transpose clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize
        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix ->
            match matrix with
            | MatrixCOO m -> COOtranspose processor m |> MatrixCOO
            | MatrixCSR m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  Rows = copy processor m.Columns
                  ColumnPointers = copy processor m.RowPointers
                  Values = copyData processor m.Values }
                |> MatrixCSC
            | MatrixCSC m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  RowPointers = copy processor m.ColumnPointers
                  Columns = copy processor m.Rows
                  Values = copyData processor m.Values }
                |> MatrixCSR

    let mxm
        (opAdd: Expr<'c -> 'c -> 'c option>)
        (opMul: Expr<'a -> 'b -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =

        let runCSRnCSC =
            CSRMatrix.spgemmCSC clContext workGroupSize opAdd opMul

        fun (queue: MailboxProcessor<_>) (matrix1: Matrix<'a>) (matrix2: Matrix<'b>) (mask: Mask2D) ->

            match matrix1, matrix2, mask.IsComplemented with
            | MatrixCSR m1, MatrixCSC m2, false -> runCSRnCSC queue m1 m2 mask |> MatrixCOO
            | _ -> failwith "Matrix formats are not matching"
