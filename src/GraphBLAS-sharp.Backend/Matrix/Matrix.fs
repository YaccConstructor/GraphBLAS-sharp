namespace GraphBLAS.FSharp

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClMatrix

[<RequireQualifiedAccess>]
module Matrix =
    /// <summary>
    /// Creates new matrix with the values from the given one.
    /// New matrix represented in the format of the given one.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let copy (clContext: ClContext) workGroupSize =

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        let vectorCopy =
            Sparse.Vector.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO m ->
                ClMatrix.COO
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      Rows = copy processor allocationMode m.Rows
                      Columns = copy processor allocationMode m.Columns
                      Values = copyData processor allocationMode m.Values }
            | ClMatrix.CSR m ->
                ClMatrix.CSR
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      RowPointers = copy processor allocationMode m.RowPointers
                      Columns = copy processor allocationMode m.Columns
                      Values = copyData processor allocationMode m.Values }
            | ClMatrix.CSC m ->
                ClMatrix.CSC
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      Rows = copy processor allocationMode m.Rows
                      ColumnPointers = copy processor allocationMode m.ColumnPointers
                      Values = copyData processor allocationMode m.Values }
            | ClMatrix.LIL matrix ->
                matrix.Rows
                |> List.map (Option.map (vectorCopy processor allocationMode))
                |> fun rows ->
                    { Context = clContext
                      RowCount = matrix.RowCount
                      ColumnCount = matrix.ColumnCount
                      Rows = rows
                      NNZ = matrix.NNZ }
                    |> ClMatrix.LIL

    /// <summary>
    /// Creates a new matrix, represented in CSR format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSR (clContext: ClContext) workGroupSize =
        let toCSR = COO.Matrix.toCSR clContext workGroupSize

        let copy = copy clContext workGroupSize

        let transpose =
            CSR.Matrix.transpose clContext workGroupSize

        let rowsToCSR = LIL.Matrix.toCSR clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO m -> toCSR processor allocationMode m |> ClMatrix.CSR
            | ClMatrix.CSR _ -> copy processor allocationMode matrix
            | ClMatrix.CSC m ->
                m.ToCSR
                |> transpose processor allocationMode
                |> ClMatrix.CSR
            | ClMatrix.LIL m ->
                rowsToCSR processor allocationMode m
                |> ClMatrix.CSR

    /// <summary>
    /// Returns the matrix, represented in CSR format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSRInPlace (clContext: ClContext) workGroupSize =
        let toCSRInPlace =
            COO.Matrix.toCSRInPlace clContext workGroupSize

        let transposeInPlace =
            CSR.Matrix.transposeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO m ->
                toCSRInPlace processor allocationMode m
                |> ClMatrix.CSR
            | ClMatrix.CSR _ -> matrix
            | ClMatrix.CSC m ->
                m.ToCSR
                |> transposeInPlace processor allocationMode
                |> ClMatrix.CSR
            | _ -> failwith "Not yet implemented"

    /// <summary>
    /// Creates a new matrix, represented in COO format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCOO (clContext: ClContext) workGroupSize =
        let toCOO = CSR.Matrix.toCOO clContext workGroupSize

        let copy = copy clContext workGroupSize

        let transposeInPlace =
            COO.Matrix.transposeInPlace clContext workGroupSize

        let rowsToCSR = LIL.Matrix.toCSR clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO _ -> copy processor allocationMode matrix
            | ClMatrix.CSR m -> toCOO processor allocationMode m |> ClMatrix.COO
            | ClMatrix.CSC m ->
                m.ToCSR
                |> toCOO processor allocationMode
                |> transposeInPlace processor
                |> ClMatrix.COO
            | ClMatrix.LIL m ->
                rowsToCSR processor allocationMode m
                |> toCOO processor allocationMode
                |> ClMatrix.COO

    /// <summary>
    /// Returns the matrix, represented in COO format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCOOInPlace (clContext: ClContext) workGroupSize =
        let toCOOInPlace =
            CSR.Matrix.toCOOInPlace clContext workGroupSize

        let transposeInPlace =
            COO.Matrix.transposeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO _ -> matrix
            | ClMatrix.CSR m ->
                toCOOInPlace processor allocationMode m
                |> ClMatrix.COO
            | ClMatrix.CSC m ->
                m.ToCSR
                |> toCOOInPlace processor allocationMode
                |> transposeInPlace processor
                |> ClMatrix.COO
            | _ -> failwith "Not yet implemented"

    /// <summary>
    /// Creates a new matrix, represented in CSC format, that is equal to the given one.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSC (clContext: ClContext) workGroupSize =
        let COOtoCSR = COO.Matrix.toCSR clContext workGroupSize

        let copy = copy clContext workGroupSize

        let transposeCSR =
            CSR.Matrix.transpose clContext workGroupSize

        let transposeCOO =
            COO.Matrix.transpose clContext workGroupSize

        let rowsToCSR = LIL.Matrix.toCSR clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.CSC _ -> copy processor allocationMode matrix
            | ClMatrix.CSR m ->
                (transposeCSR processor allocationMode m).ToCSC
                |> ClMatrix.CSC
            | ClMatrix.COO m ->
                (transposeCOO processor allocationMode m
                 |> COOtoCSR processor allocationMode)
                    .ToCSC
                |> ClMatrix.CSC
            | ClMatrix.LIL m ->
                rowsToCSR processor allocationMode m
                |> transposeCSR processor allocationMode
                |> fun m -> m.ToCSC
                |> ClMatrix.CSC

    /// <summary>
    /// Returns the matrix, represented in CSC format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSCInPlace (clContext: ClContext) workGroupSize =
        let toCSRInPlace =
            COO.Matrix.toCSRInPlace clContext workGroupSize

        let transposeCSRInPlace =
            CSR.Matrix.transposeInPlace clContext workGroupSize

        let transposeCOOInPlace =
            COO.Matrix.transposeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.CSC _ -> matrix
            | ClMatrix.CSR m ->
                (transposeCSRInPlace processor allocationMode m)
                    .ToCSC
                |> ClMatrix.CSC
            | ClMatrix.COO m ->
                (transposeCOOInPlace processor m
                 |> toCSRInPlace processor allocationMode)
                    .ToCSC
                |> ClMatrix.CSC
            | _ -> failwith "Not yet implemented"

    /// <summary>
    /// Creates a new matrix, represented in LIL format, that is equal to the given one.
    /// </summary>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toLIL (clContext: ClContext) workGroupSize =

        let copy = copy clContext workGroupSize

        let COOToCSR = COO.Matrix.toCSR clContext workGroupSize

        let transposeCSR =
            CSR.Matrix.transpose clContext workGroupSize

        let CSRToLIL = CSR.Matrix.toLIL clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.CSC m ->
                m.ToCSR
                |> transposeCSR processor allocationMode
                |> CSRToLIL processor allocationMode
                |> ClMatrix.LIL
            | ClMatrix.CSR m ->
                CSRToLIL processor allocationMode m
                |> ClMatrix.LIL
            | ClMatrix.COO m ->
                COOToCSR processor allocationMode m
                |> CSRToLIL processor allocationMode
                |> ClMatrix.LIL
            | ClMatrix.LIL _ -> copy processor allocationMode matrix

    /// <summary>
    /// Builds a new matrix whose elements are the results of applying the given function
    /// to each of the elements of the matrix.
    /// </summary>
    /// <param name="op">
    /// A function to transform values of the input matrix.
    /// Operand and result types should be optional to distinguish explicit and implicit zeroes
    /// </param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let map (op: Expr<'a option -> 'b option>) (clContext: ClContext) workGroupSize =
        let mapCOO =
            COO.Matrix.map op clContext workGroupSize

        let mapCSR =
            CSR.Matrix.map op clContext workGroupSize

        let transposeCOO =
            COO.Matrix.transposeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode matrix ->
            match matrix with
            | ClMatrix.COO m -> mapCOO processor allocationMode m |> ClMatrix.COO
            | ClMatrix.CSR m -> mapCSR processor allocationMode m |> ClMatrix.COO
            | ClMatrix.CSC m ->
                (mapCSR processor allocationMode m.ToCSR)
                |> transposeCOO processor
                |> ClMatrix.COO
            | _ -> failwith "Not yet implemented"

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
    let transposeInPlace (clContext: ClContext) workGroupSize =
        let COOTransposeInPlace =
            COO.Matrix.transposeInPlace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix ->
            match matrix with
            | ClMatrix.COO m -> COOTransposeInPlace processor m |> ClMatrix.COO
            | ClMatrix.CSR m -> ClMatrix.CSC m.ToCSC
            | ClMatrix.CSC m -> ClMatrix.CSR m.ToCSR
            | ClMatrix.LIL _ -> failwith "Not yet implemented"

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
        let COOTranspose =
            COO.Matrix.transpose clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode matrix ->
            match matrix with
            | ClMatrix.COO m ->
                COOTranspose processor allocationMode m
                |> ClMatrix.COO
            | ClMatrix.CSR m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  Rows = copy processor allocationMode m.Columns
                  ColumnPointers = copy processor allocationMode m.RowPointers
                  Values = copyData processor allocationMode m.Values }
                |> ClMatrix.CSC
            | ClMatrix.CSC m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  RowPointers = copy processor allocationMode m.ColumnPointers
                  Columns = copy processor allocationMode m.Rows
                  Values = copyData processor allocationMode m.Values }
                |> ClMatrix.CSR
            | ClMatrix.LIL _ -> failwith "Not yet implemented"
