namespace GraphBLAS.FSharp.Backend.Matrix

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix

module Matrix =
    let copy (clContext: ClContext) workGroupSize =
        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

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

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO m -> toCSR processor allocationMode m |> ClMatrix.CSR
            | ClMatrix.CSR _ -> copy processor allocationMode matrix
            | ClMatrix.CSC m ->
                m.ToCSR
                |> transpose processor allocationMode
                |> ClMatrix.CSR

    /// <summary>
    /// Returns the matrix, represented in CSR format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSRInplace (clContext: ClContext) workGroupSize =
        let toCSRInplace =
            COO.Matrix.toCSRInplace clContext workGroupSize

        let transposeInplace =
            CSR.Matrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO m ->
                toCSRInplace processor allocationMode m
                |> ClMatrix.CSR
            | ClMatrix.CSR _ -> matrix
            | ClMatrix.CSC m ->
                m.ToCSR
                |> transposeInplace processor allocationMode
                |> ClMatrix.CSR

    /// <summary>
    /// Creates a new matrix, represented in COO format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCOO (clContext: ClContext) workGroupSize =
        let toCOO = CSR.Matrix.toCOO clContext workGroupSize

        let copy = copy clContext workGroupSize

        let transposeInplace =
            COO.Matrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO _ -> copy processor allocationMode matrix
            | ClMatrix.CSR m -> toCOO processor allocationMode m |> ClMatrix.COO
            | ClMatrix.CSC m ->
                m.ToCSR
                |> toCOO processor allocationMode
                |> transposeInplace processor
                |> ClMatrix.COO

    /// <summary>
    /// Returns the matrix, represented in COO format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCOOInplace (clContext: ClContext) workGroupSize =
        let toCOOInplace =
            CSR.Matrix.toCOOInplace clContext workGroupSize

        let transposeInplace =
            COO.Matrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO _ -> matrix
            | ClMatrix.CSR m ->
                toCOOInplace processor allocationMode m
                |> ClMatrix.COO
            | ClMatrix.CSC m ->
                m.ToCSR
                |> toCOOInplace processor allocationMode
                |> transposeInplace processor
                |> ClMatrix.COO

    /// <summary>
    /// Creates a new matrix, represented in CSC format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSC (clContext: ClContext) workGroupSize =
        let toCSR = COO.Matrix.toCSR clContext workGroupSize

        let copy = copy clContext workGroupSize

        let transposeCSR =
            CSR.Matrix.transpose clContext workGroupSize

        let transposeCOO =
            COO.Matrix.transpose clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.CSC _ -> copy processor allocationMode matrix
            | ClMatrix.CSR m ->
                (transposeCSR processor allocationMode m).ToCSC
                |> ClMatrix.CSC
            | ClMatrix.COO m ->
                (transposeCOO processor allocationMode m
                 |> toCSR processor allocationMode)
                    .ToCSC
                |> ClMatrix.CSC

    /// <summary>
    /// Returns the matrix, represented in CSC format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSCInplace (clContext: ClContext) workGroupSize =
        let toCSRInplace =
            COO.Matrix.toCSRInplace clContext workGroupSize

        let transposeCSRInplace =
            CSR.Matrix.transposeInplace clContext workGroupSize

        let transposeCOOInplace =
            COO.Matrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.CSC _ -> matrix
            | ClMatrix.CSR m ->
                (transposeCSRInplace processor allocationMode m)
                    .ToCSC
                |> ClMatrix.CSC
            | ClMatrix.COO m ->
                (transposeCOOInplace processor m
                 |> toCSRInplace processor allocationMode)
                    .ToCSC
                |> ClMatrix.CSC

    let map (clContext: ClContext) (opAdd: Expr<'a option -> 'b option>) workGroupSize =
        let mapCOO =
            COO.Matrix.map clContext opAdd workGroupSize

        let mapCSR =
            CSR.Matrix.map clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode matrix ->
            match matrix with
            | ClMatrix.COO m -> mapCOO processor allocationMode m |> ClMatrix.COO
            | ClMatrix.CSR m -> mapCSR processor allocationMode m |> ClMatrix.CSR
            | ClMatrix.CSC m ->
                (mapCSR processor allocationMode m.ToCSR).ToCSC
                |> ClMatrix.CSC

    let map2 (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize = // TODO()
        let map2COO =
            COO.Matrix.map2 clContext opAdd workGroupSize

        let map2CSR =
            CSR.Matrix.map2 clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrix.COO m1, ClMatrix.COO m2 ->
                map2COO processor allocationMode m1 m2
                |> ClMatrix.COO
            | ClMatrix.CSR m1, ClMatrix.CSR m2 ->
                map2CSR processor allocationMode m1 m2
                |> ClMatrix.CSR
            | ClMatrix.CSC m1, ClMatrix.CSC m2 ->
                (map2CSR processor allocationMode m1.ToCSR m2.ToCSR)
                    .ToCSC
                |> ClMatrix.CSC
            | _ -> failwith "Matrix formats are not matching"

    let map2AtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =
        let COOElementwise =
            COO.Matrix.map2AtLeastOne clContext opAdd workGroupSize

        let CSRElementwise =
            CSR.Matrix.map2AtLeastOne clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrix.COO m1, ClMatrix.COO m2 ->
                COOElementwise processor allocationMode m1 m2
                |> ClMatrix.COO
            | ClMatrix.CSR m1, ClMatrix.CSR m2 ->
                CSRElementwise processor allocationMode m1 m2
                |> ClMatrix.CSR
            | ClMatrix.CSC m1, ClMatrix.CSC m2 ->
                (CSRElementwise processor allocationMode m1.ToCSR m2.ToCSR)
                    .ToCSC
                |> ClMatrix.CSC
            | _ -> failwith "Matrix formats are not matching"

    let map2AtLeastOneToCOO (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize =
        let COOElementwise =
            COO.Matrix.map2AtLeastOne clContext opAdd workGroupSize

        let CSRElementwise =
            CSR.Matrix.map2AtLeastOneToCOO clContext opAdd workGroupSize

        let transposeCOOInplace =
            COO.Matrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrix.COO m1, ClMatrix.COO m2 ->
                COOElementwise processor allocationMode m1 m2
                |> ClMatrix.COO
            | ClMatrix.CSR m1, ClMatrix.CSR m2 ->
                CSRElementwise processor allocationMode m1 m2
                |> ClMatrix.COO
            | ClMatrix.CSC m1, ClMatrix.CSC m2 ->
                CSRElementwise processor allocationMode m1.ToCSR m2.ToCSR
                |> transposeCOOInplace processor
                |> ClMatrix.COO
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
            COO.Matrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix ->
            match matrix with
            | ClMatrix.COO m -> COOtransposeInplace processor m |> ClMatrix.COO
            | ClMatrix.CSR m -> ClMatrix.CSC m.ToCSC
            | ClMatrix.CSC m -> ClMatrix.CSR m.ToCSR

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
            COO.Matrix.transpose clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode matrix ->
            match matrix with
            | ClMatrix.COO m ->
                COOtranspose processor allocationMode m
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

    let kronecker (op: Expr<'a option -> 'b option -> 'c option>) (clContext: ClContext) workGroupSize =
        let run =
            CSR.Matrix.kronecker clContext op workGroupSize

        fun (queue: MailboxProcessor<_>) allocationFlag (matrix1: ClMatrix<'a>) (matrix2: ClMatrix<'b>) ->
            match matrix1, matrix2 with
            | ClMatrix.CSR m1, ClMatrix.CSR m2 ->
                let result = run queue allocationFlag m1 m2
                Option.bind (Some << ClMatrix.COO) result
            | _ -> failwith "Matrix formats are not matching"

    module SpGeMM =
        let masked
            (opAdd: Expr<'c -> 'c -> 'c option>)
            (opMul: Expr<'a -> 'b -> 'c option>)
            (clContext: ClContext)
            workGroupSize
            =

            let runCSRnCSC =
                CSR.Matrix.SpGeMM.masked clContext workGroupSize opAdd opMul

            fun (queue: MailboxProcessor<_>) (matrix1: ClMatrix<'a>) (matrix2: ClMatrix<'b>) (mask: ClMatrix<_>) ->
                match matrix1, matrix2, mask with
                | ClMatrix.CSR m1, ClMatrix.CSC m2, ClMatrix.COO mask -> runCSRnCSC queue m1 m2 mask |> ClMatrix.COO
                | _ -> failwith "Matrix formats are not matching"

        let expand
            (clContext: ClContext)
            workGroupSize
            (opAdd: Expr<'c -> 'c -> 'c option>)
            (opMul: Expr<'a -> 'b -> 'c option>)
            =

            let run =
                CSR.Matrix.SpGeMM.expand clContext workGroupSize opAdd opMul

            fun (processor: MailboxProcessor<_>) allocationMode (leftMatrix: ClMatrix<'a>) (rightMatrix: ClMatrix<'b>) ->
                match leftMatrix, rightMatrix with
                | ClMatrix.CSR leftMatrix, ClMatrix.CSR rightMatrix ->
                    run processor allocationMode leftMatrix rightMatrix
                    |> ClMatrix.COO
                | _ -> failwith "Matrix formats are not matching"
