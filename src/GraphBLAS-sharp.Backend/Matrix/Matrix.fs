namespace GraphBLAS.FSharp.Backend.Matrix

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Matrix.COO
open GraphBLAS.FSharp.Backend.Matrix.CSR
open GraphBLAS.FSharp.Backend.Objects

module Matrix =
    let copy (clContext: ClContext) workGroupSize =
        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO m ->
                let res =
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      Rows = copy processor m.Rows
                      Columns = copy processor m.Columns
                      Values = copyData processor m.Values }

                ClMatrixCOO res
            | ClMatrixCSR m ->
                let res =
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      RowPointers = copy processor m.RowPointers
                      Columns = copy processor m.Columns
                      Values = copyData processor m.Values }

                ClMatrixCSR res
            | ClMatrixCSC m ->
                let res =
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      Rows = copy processor m.Rows
                      ColumnPointers = copy processor m.ColumnPointers
                      Values = copyData processor m.Values }

                ClMatrixCSC res

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

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO m -> toCSR processor m |> ClMatrixCSR
            | ClMatrixCSR _ -> copy processor matrix
            | ClMatrixCSC m ->
                let csrT =
                    { Context = m.Context
                      RowCount = m.ColumnCount
                      ColumnCount = m.RowCount
                      RowPointers = m.ColumnPointers
                      Columns = m.Rows
                      Values = m.Values }

                transpose processor csrT |> ClMatrixCSR

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

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO m -> toCSRInplace processor m |> ClMatrixCSR
            | ClMatrixCSR _ -> matrix
            | ClMatrixCSC m ->
                let csrT =
                    { Context = m.Context
                      RowCount = m.ColumnCount
                      ColumnCount = m.RowCount
                      RowPointers = m.ColumnPointers
                      Columns = m.Rows
                      Values = m.Values }

                transposeInplace processor csrT |> ClMatrixCSR

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

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO _ -> copy processor matrix
            | ClMatrixCSR m -> toCOO processor m |> ClMatrixCOO
            | ClMatrixCSC m ->
                let csrT =
                    { Context = m.Context
                      RowCount = m.ColumnCount
                      ColumnCount = m.RowCount
                      RowPointers = m.ColumnPointers
                      Columns = m.Rows
                      Values = m.Values }

                let cooT = toCOO processor csrT
                transposeInplace processor cooT |> ClMatrixCOO

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

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCOO _ -> matrix
            | ClMatrixCSR m -> toCOOInplace processor m |> ClMatrixCOO
            | ClMatrixCSC m ->
                let csrT =
                    { Context = m.Context
                      RowCount = m.ColumnCount
                      ColumnCount = m.RowCount
                      RowPointers = m.ColumnPointers
                      Columns = m.Rows
                      Values = m.Values }

                let cooT = toCOOInplace processor csrT
                transposeInplace processor cooT |> ClMatrixCOO

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

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCSC _ -> copy processor matrix
            | ClMatrixCSR m ->
                let csrT = transposeCSR processor m

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> ClMatrixCSC
            | ClMatrixCOO m ->
                let cooT = transposeCOO processor m
                let csrT = toCSR processor cooT

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> ClMatrixCSC

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

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrixCSC _ -> matrix
            | ClMatrixCSR m ->
                let csrT = transposeCSRInplace processor m

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> ClMatrixCSC
            | ClMatrixCOO m ->
                let cooT = transposeCOOInplace processor m
                let csrT = toCSRInplace processor cooT

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> ClMatrixCSC

    let elementwise (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =
        let COOElementwise =
            COOMatrix.elementwise clContext opAdd workGroupSize

        let CSRElementwise =
            CSRMatrix.elementwise clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrixCOO m1, ClMatrixCOO m2 -> COOElementwise processor m1 m2 |> ClMatrixCOO
            | ClMatrixCSR m1, ClMatrixCSR m2 -> CSRElementwise processor m1 m2 |> ClMatrixCSR
            | ClMatrixCSC m1, ClMatrixCSC m2 ->
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

                let resT = CSRElementwise processor csrT1 csrT2

                { Context = resT.Context
                  RowCount = resT.ColumnCount
                  ColumnCount = resT.RowCount
                  Rows = resT.Columns
                  ColumnPointers = resT.RowPointers
                  Values = resT.Values }
                |> ClMatrixCSC
            | _ -> failwith "Matrix formats are not matching"

    let elementwiseToCOO (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize =
        let COOElementwise =
            COOMatrix.elementwise clContext opAdd workGroupSize

        let CSRElementwise =
            CSRMatrix.elementwiseToCOO clContext opAdd workGroupSize

        let transposeCOOInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrixCOO m1, ClMatrixCOO m2 -> COOElementwise processor m1 m2 |> ClMatrixCOO
            | ClMatrixCSR m1, ClMatrixCSR m2 -> CSRElementwise processor m1 m2 |> ClMatrixCOO
            | ClMatrixCSC m1, ClMatrixCSC m2 ->
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

                let resT = CSRElementwise processor csrT1 csrT2
                ClMatrixCOO <| transposeCOOInplace processor resT
            | _ -> failwith "Matrix formats are not matching"

    let elementwiseAtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =
        let COOElementwise =
            COOMatrix.elementwiseAtLeastOne clContext opAdd workGroupSize

        let CSRElementwise =
            CSRMatrix.elementwiseAtLeastOne clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrixCOO m1, ClMatrixCOO m2 -> COOElementwise processor m1 m2 |> ClMatrixCOO
            | ClMatrixCSR m1, ClMatrixCSR m2 -> CSRElementwise processor m1 m2 |> ClMatrixCSR
            | ClMatrixCSC m1, ClMatrixCSC m2 ->
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

                let resT = CSRElementwise processor csrT1 csrT2

                { Context = resT.Context
                  RowCount = resT.ColumnCount
                  ColumnCount = resT.RowCount
                  Rows = resT.Columns
                  ColumnPointers = resT.RowPointers
                  Values = resT.Values }
                |> ClMatrixCSC
            | _ -> failwith "Matrix formats are not matching"

    let elementwiseAtLeastOneToCOO (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =
        let COOElementwise =
            COOMatrix.elementwiseAtLeastOne clContext opAdd workGroupSize

        let CSRElementwise =
            CSRMatrix.elementwiseAtLeastOneToCOO clContext opAdd workGroupSize

        let transposeCOOInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrixCOO m1, ClMatrixCOO m2 -> COOElementwise processor m1 m2 |> ClMatrixCOO
            | ClMatrixCSR m1, ClMatrixCSR m2 -> CSRElementwise processor m1 m2 |> ClMatrixCOO
            | ClMatrixCSC m1, ClMatrixCSC m2 ->
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

                let resT = CSRElementwise processor csrT1 csrT2
                ClMatrixCOO <| transposeCOOInplace processor resT
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
            | ClMatrixCOO m -> COOtransposeInplace processor m |> ClMatrixCOO
            | ClMatrixCSR m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  Rows = m.Columns
                  ColumnPointers = m.RowPointers
                  Values = m.Values }
                |> ClMatrixCSC
            | ClMatrixCSC m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  RowPointers = m.ColumnPointers
                  Columns = m.Rows
                  Values = m.Values }
                |> ClMatrixCSR

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
            | ClMatrixCOO m -> COOtranspose processor m |> ClMatrixCOO
            | ClMatrixCSR m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  Rows = copy processor m.Columns
                  ColumnPointers = copy processor m.RowPointers
                  Values = copyData processor m.Values }
                |> ClMatrixCSC
            | ClMatrixCSC m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  RowPointers = copy processor m.ColumnPointers
                  Columns = copy processor m.Rows
                  Values = copyData processor m.Values }
                |> ClMatrixCSR

    let mxm
        (opAdd: Expr<'c -> 'c -> 'c option>)
        (opMul: Expr<'a -> 'b -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =

        let runCSRnCSC =
            CSRMatrix.spgemmCSC clContext workGroupSize opAdd opMul

        fun (queue: MailboxProcessor<_>) (matrix1: ClMatrix<'a>) (matrix2: ClMatrix<'b>) (mask: ClMask2D) ->

            match matrix1, matrix2, mask.IsComplemented with
            | ClMatrixCSR m1, ClMatrixCSC m2, false -> runCSRnCSC queue m1 m2 mask |> ClMatrixCOO
            | _ -> failwith "Matrix formats are not matching"
